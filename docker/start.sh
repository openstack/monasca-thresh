#!/bin/ash
# shellcheck shell=dash

if [ -n "$DEBUG" ]; then
  set -x
fi

CONFIG_TEMPLATES="/templates"
CONFIG_DEST="/etc/monasca"
LOG_TEMPLATES="/logging"
LOG_DEST="/storm/log4j2"
APACHE_STORM_DIR="/apache-storm-1.1.1"

ZOOKEEPER_WAIT=${ZOOKEEPER_WAIT:-"true"}
ZOOKEEPER_WAIT_TIMEOUT=${ZOOKEEPER_WAIT_TIMEOUT:-"3"}
ZOOKEEPER_WAIT_DELAY=${ZOOKEEPER_WAIT_DELAY:-"10"}
ZOOKEEPER_WAIT_RETRIES=${ZOOKEEPER_WAIT_RETRIES:-"20"}

SUPERVISOR_STACK_SIZE=${SUPERVISOR_STACK_SIZE:-"1024k"}
WORKER_STACK_SIZE=${WORKER_STACK_SIZE:-"1024k"}
NIMBUS_STACK_SIZE=${NIMBUS_STACK_SIZE:-"1024k"}
UI_STACK_SIZE=${UI_STACK_SIZE:-"1024k"}

TOPOLOGY_NAME="thresh-cluster"

MYSQL_WAIT_RETRIES=${MYSQL_WAIT_RETRIES:-"24"}
MYSQL_WAIT_DELAY=${MYSQL_WAIT_DELAY:-"5"}

KAFKA_WAIT_RETRIES=${KAFKA_WAIT_RETRIES:-"24"}
KAFKA_WAIT_DELAY=${KAFKA_WAIT_DELAY:-"5"}

THRESH_STACK_SIZE=${THRESH_STACK_SIZE:-"1024k"}

if [ -n "$ZOOKEEPER_SERVERS" ]; then
  if [ -z "$STORM_ZOOKEEPER_SERVERS" ]; then
    export STORM_ZOOKEEPER_SERVERS="$ZOOKEEPER_SERVERS"
  fi

  if [ -z "$TRANSACTIONAL_ZOOKEEPER_SERVERS" ]; then
    export TRANSACTIONAL_ZOOKEEPER_SERVERS="$ZOOKEEPER_SERVERS"
  fi
fi

if [ -n "$ZOOKEEPER_PORT" ]; then
  if [ -z "$STORM_ZOOKEEPER_PORT" ]; then
    export STORM_ZOOKEEPER_PORT="$ZOOKEEPER_PORT"
  fi

  if [ -z "$TRANSACTIONAL_ZOOKEEPER_PORT" ]; then
    export TRANSACTIONAL_ZOOKEEPER_PORT="$ZOOKEEPER_PORT"
  fi
fi

first_zk=$(echo "$STORM_ZOOKEEPER_SERVERS" | cut -d, -f1)

# wait for zookeeper to become available
if [ "$ZOOKEEPER_WAIT" = "true" ]; then
  success="false"
  for i in $(seq "$ZOOKEEPER_WAIT_RETRIES"); do
    if ok=$(echo ruok | nc "$first_zk" "$STORM_ZOOKEEPER_PORT" -w "$ZOOKEEPER_WAIT_TIMEOUT") && [ "$ok" = "imok" ]; then
      success="true"
      break
    else
      echo "Connect attempt $i of $ZOOKEEPER_WAIT_RETRIES failed, retrying..."
      sleep "$ZOOKEEPER_WAIT_DELAY"
    fi
  done

  if [ "$success" != "true" ]; then
    echo "Could not connect to $first_zk after $i attempts, exiting..."
    sleep 1
    exit 1
  fi
fi

if [ -z "$STORM_LOCAL_HOSTNAME" ]; then
  # see also: http://stackoverflow.com/a/21336679
  ip=$(ip route get 8.8.8.8 | awk 'NR==1 {print $NF}')
  echo "Using autodetected IP as advertised hostname: $ip"
  export STORM_LOCAL_HOSTNAME=$ip
fi

if [ -z "$SUPERVISOR_CHILDOPTS" ]; then
  SUPERVISOR_CHILDOPTS="-XX:MaxRAM=$(python /memory.py "$SUPERVISOR_MAX_MB") -XX:+UseSerialGC -Xss$SUPERVISOR_STACK_SIZE"
  export SUPERVISOR_CHILDOPTS
fi

if [ -z "$WORKER_CHILDOPTS" ]; then
  WORKER_CHILDOPTS="-XX:MaxRAM=$(python /memory.py "$WORKER_MAX_MB") -Xss$WORKER_STACK_SIZE"
  WORKER_CHILDOPTS="$WORKER_CHILDOPTS -XX:+UseConcMarkSweepGC"
  if [ "$WORKER_REMOTE_JMX" = "true" ]; then
    WORKER_CHILDOPTS="$WORKER_CHILDOPTS -Dcom.sun.management.jmxremote"
  fi

  export WORKER_CHILDOPTS
fi

if [ -z "$NIMBUS_CHILDOPTS" ]; then
  NIMBUS_CHILDOPTS="-XX:MaxRAM=$(python /memory.py "$NIMBUS_MAX_MB") -XX:+UseSerialGC -Xss$NIMBUS_STACK_SIZE"
  export NIMBUS_CHILDOPTS
fi

if [ -z "$UI_CHILDOPTS" ]; then
  UI_CHILDOPTS="-XX:MaxRAM=$(python /memory.py "$UI_MAX_MB") -XX:+UseSerialGC -Xss$UI_STACK_SIZE"
  export UI_CHILDOPTS
fi

template_dir() {
  src_dir=$1
  dest_dir=$2

  for f in "$src_dir"/*; do
     # Skip directories, links, etc
    if [ ! -f "$f" ]; then
      continue
    fi

    name=$(basename "$f")
    dest=$(basename "$f" .j2)
    if [ "$dest" = "$name" ]; then
      # file does not end in .j2
      cp "$f" "$dest_dir/$dest"
    else
      # file ends in .j2, apply template
      templer --verbose --force "$f" "$dest_dir/$dest"
    fi
  done
}

templer --verbose --force "$CONFIG_TEMPLATES/storm.yaml.j2" "$STORM_CONF_DIR/storm.yaml"

template_dir "$CONFIG_TEMPLATES" "$CONFIG_DEST"
template_dir "$LOG_TEMPLATES" "$LOG_DEST"

if [ "$WORKER_LOGS_TO_STDOUT" = "true" ]; then
  for PORT in $(echo "$SUPERVISOR_SLOTS_PORTS" | sed -e "s/,/ /"); do
    LOGDIR="/storm/logs/workers-artifacts/thresh/$PORT"
    mkdir -p "$LOGDIR"
    WORKER_LOG="$LOGDIR/worker.log"
    RECREATE="true"
    if [ -e "$WORKER_LOG" ]; then
      if [ -L "$WORKER_LOG" ]; then
        RECREATE="false"
      else
        rm -f "$WORKER_LOG"
      fi
    fi
    if [ $RECREATE = "true" ]; then
      ln -s /proc/1/fd/1 "$WORKER_LOG"
    fi
  done
fi

# Test services we need before starting our service.
echo "Start script: waiting for needed services"
python3 /kafka_wait_for_topics.py
python3 /mysql_check.py


if [ "${NO_STORM_CLUSTER}" = "true" ]; then
  echo "Using Thresh Config file /etc/monasca/thresh-config.yml. Contents:"
  grep -vi password /etc/monasca/thresh-config.yml
  # shellcheck disable=SC2086
  JAVAOPTS="-XX:MaxRAM=$(python /memory.py $WORKER_MAX_MB) -XX:+UseSerialGC -Xss$THRESH_STACK_SIZE"

  if [ "$LOCAL_JMX" = "true" ]; then
    JAVAOPTS="$JAVAOPTS -Dcom.sun.management.jmxremote=true"

    port="${LOCAL_JMX_PORT:-9090}"
    JAVAOPTS="$JAVAOPTS -Dcom.sun.management.jmxremote.port=$port"
    JAVAOPTS="$JAVAOPTS -Dcom.sun.management.jmxremote.rmi.port=$port"
    JAVAOPTS="$JAVAOPTS -Dcom.sun.management.jmxremote.ssl=false"
    JAVAOPTS="$JAVAOPTS -Dcom.sun.management.jmxremote.authenticate=false"
    JAVAOPTS="$JAVAOPTS -Dcom.sun.management.jmxremote.local.only=false"
  fi

  if [ -n "$LOG_CONFIG_FILE" ]; then
    JAVAOPTS="$JAVAOPTS -Dlog4j.configurationFile=$LOG_CONFIG_FILE"
  fi

  echo "Submitting storm topology as local cluster using JAVAOPTS of $JAVAOPTS"
  # shellcheck disable=SC2086
  export STORM_JAR_JVM_OPTS="$JAVAOPTS"
  exec storm jar "/monasca-thresh.jar" monasca.thresh.ThresholdingEngine /etc/monasca/thresh-config.yml thresh-cluster local
  exit $?
fi

echo "Waiting for storm to become available..."
success="false"
for i in $(seq "$STORM_WAIT_RETRIES"); do
  if timeout -t "$STORM_WAIT_TIMEOUT" storm list; then
    echo "Storm is available, continuing..."
    success="true"
    break
  else
    echo "Connection attempt $i of $STORM_WAIT_RETRIES failed"
    sleep "$STORM_WAIT_DELAY"
  fi
done

if [ "$success" != "true" ]; then
  echo "Unable to connect to Storm! Exiting..."
  sleep 1
  exit 1
fi

topologies=$(storm list | awk '/-----/,0{if (!/-----/)print $1}')
found="false"
for topology in $topologies; do
  if [ "$topology" = "$TOPOLOGY_NAME" ]; then
    found="true"
    echo "Found existing storm topology with name: $topology"
    break
  fi
done

if [ "$found" = "true" ]; then
  echo "Storm topology already exists, will not submit again"
  # TODO handle upgrades
else
  echo "Using Thresh Config file /etc/monasca/thresh-config.yml. Contents:"
  grep -vi password /etc/monasca/thresh-config.yml
  echo "Submitting storm topology..."
  storm jar /monasca-thresh.jar \
    monasca.thresh.ThresholdingEngine \
    /etc/monasca/thresh-config.yml \
    "$TOPOLOGY_NAME"
fi

# Template all config files before start, it will use env variables.
# Read usage examples: https://pypi.org/project/Templer/
echo "Start script: creating config files from templates"
# Add proxy configuration for maven
mkdir -p /root/.m2
templer --ignore-undefined-variables --verbose --force \
  /settings.xml.j2 /root/.m2/settings.xml
