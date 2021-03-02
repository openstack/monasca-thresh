ARG DOCKER_IMAGE=monasca/thresh
ARG APP_REPO=https://review.opendev.org/openstack/monasca-thresh

# Branch, tag or git hash to build from.
ARG REPO_VERSION=master
ARG CONSTRAINTS_BRANCH=master

FROM storm:1.2.3

ENV \
    MAVEN_HOME="/usr/share/maven" \
    JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64" \
    ZOOKEEPER_SERVERS="zookeeper" \
    ZOOKEEPER_PORT="2181" \
    ZOOKEEPER_WAIT="true" \
    SUPERVISOR_SLOTS_PORTS="6701,6702" \
    SUPERVISOR_MAX_MB="256" \
    WORKER_MAX_MB="784" \
    NIMBUS_SEEDS="storm-nimbus" \
    NIMBUS_MAX_MB="256" \
    UI_MAX_MB="768" \
    WORKER_LOGS_TO_STDOUT="false" \
    USE_SSL_ENABLED="true"

COPY memory.py settings.xml.j2 /
COPY start.sh mysql_check.py kafka_wait_for_topics.py /

COPY templates /templates
COPY logging /logging

ENV \
    KAFKA_URI="kafka:9092" \
    KAFKA_WAIT_FOR_TOPICS=alarm-state-transitions,metrics,events \
    LOGSTASH_FIELDS="service=monasca-thresh" \
    LOG_CONFIG_FILE="/storm/log4j2/cluster.xml" \
    MYSQL_HOST=mysql \
    MYSQL_PORT=3306 \
    MYSQL_USER=thresh \
    MYSQL_PASSWORD=password \
    MYSQL_DB=mon \
    NO_STORM_CLUSTER=false \
    STORM_WAIT_DELAY=5 \
    STORM_WAIT_RETRIES=24 \
    STORM_WAIT_TIMEOUT=20 \
    WORKER_MAX_HEAP_MB=256

ARG SKIP_COMMON_TESTS=false
ARG SKIP_THRESH_TESTS=false

ARG CREATION_TIME
ARG DOCKER_IMAGE
ARG APP_REPO
ARG GITHUB_REPO
ARG REPO_VERSION
ARG GIT_COMMIT
ARG CONSTRAINTS_BRANCH
ARG CONSTRAINTS_FILE
ARG EXTRA_DEPS
ARG COMMON_REPO
ARG COMMON_VERSION
ARG COMMON_GIT_COMMIT

SHELL ["/bin/bash", "-eo", "pipefail", "-c"]

RUN \
    mkdir -p /usr/share/man/man1 && \
    apt-get clean && \
    apt-get update && \
    apt-get install -y --no-install-recommends iproute2 net-tools netcat unzip software-properties-common && \
    apt-add-repository -y 'deb http://ftp.de.debian.org/debian sid main' && \
    apt-add-repository -y 'deb http://security.debian.org/debian-security stretch/updates main' && \
    apt-get update && \
    apt-get install -y --no-install-recommends openjdk-8-jdk -o APT::Immediate-Configure=0 && \
    apt-get install -y --no-install-recommends maven git python3-pip && \
    apt-get install -y --no-install-recommends python3 default-mysql-client && \
    mkdir /root/.m2 && \
    pip3 install --no-cache-dir --upgrade setuptools && \
    pip3 install --no-cache-dir \
        jinja2 \
        pykafka \
        pymysql \
        Templer==1.1.4 && \
    set -x && mkdir /monasca-common && \
    git -C /monasca-common init && \
    git -C /monasca-common remote add origin "$COMMON_REPO" && \
    echo "Cloning monasca-common in version: $COMMON_VERSION" && \
    git -C /monasca-common fetch origin "$COMMON_VERSION" && \
    git -C /monasca-common reset --hard FETCH_HEAD && \
    cd /monasca-common && \
    mvn --quiet -B clean install $([ "$SKIP_COMMON_TESTS" = "true" ] && echo "-DskipTests") && \
    cd / && \
    mkdir /app && \
    git -C /app init && \
    git -C /app remote add origin "$APP_REPO" && \
    echo "Cloning app in version: $REPO_VERSION" && \
    git -C /app fetch origin "$REPO_VERSION" && \
    git -C /app reset --hard FETCH_HEAD && \
    cd /app/thresh && \
    mvn --quiet -B clean package $([ "$SKIP_THRESH_TESTS" = "true" ] && echo "-DskipTests") && \
    cp /app/thresh/target/*-SNAPSHOT-shaded.jar /monasca-thresh.jar && \
    cd / && \
    # Save info about build to `/VERSIONS` file.
    printf "App:        %s\\n" "$DOCKER_IMAGE" >> /VERSIONS && \
    printf "Repository: %s\\n" "$APP_REPO" >> /VERSIONS && \
    printf "Version:    %s\\n" "$REPO_VERSION" >> /VERSIONS && \
    printf "Revision:   %s\\n" "$GIT_COMMIT" >> /VERSIONS && \
    printf "Build date: %s\\n" "$CREATION_TIME" >> /VERSIONS && \
    printf "Monasca-common version:     %s\\n" "$COMMON_VERSION" \
        >> /VERSIONS && \
    printf "Monasca-common revision:    %s\\n" \
        "$COMMON_GIT_COMMIT" >> /VERSIONS && \
    printf "Constraints file: %s\\n" \
        "$CONSTRAINTS_FILE"?h="$CONSTRAINTS_BRANCH" >> /VERSIONS && \
    apt-get remove -y apt-utils && \
    apt-get remove -y maven git python3-pip software-properties-common && \
    apt-get -y autoremove && \
    rm -rf \
        /app \
        /monasca-common \
        /root/.cache/ \
        /root/.m2/repository  \
        /tmp/* \
        /var/cache/apt/* \
        /var/log/*

ENTRYPOINT ["/start.sh"]
