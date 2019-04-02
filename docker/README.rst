===============================
Docker image for Monasca Thresh
===============================

This image has a containerized version of the Monasca Threshold Engine. For
more information on the Monasca project, see the wiki_.

Sources: monasca-thresh_, monasca-docker_, Dockerfile_


Usage
=====

The Threshold engine requires configured instances of MySQL, Kafka,
Zookeeper, and optionally `Storm monasca-api`. In environments resembling
the official docker-compose__ or Kubernetes_ environments, this image requires
little to no configuration and can be minimally run like so:

    docker run monasca/thresh:master

Environment variables
~~~~~~~~~~~~~~~~~~~~~

============================= ======================== ============================================
 Variable                     Default                  Description
============================= ======================== ============================================
 KAFKA_URI                    kafka:9092               URI to Apache Kafka
 KAFKA_WAIT_FOR_TOPICS        alarm-state-transitions, Comma-separated list of topic names to check
                              metrics,events
 KAFKA_WAIT_RETRIES           24                       Number of Kafka connection attempts
 KAFKA_WAIT_DELAY             5                        Seconds to wait between attempts
 MYSQL_HOST                   mysql                    MySQL hostname
 MYSQL_PORT                   3306                     MySQL port
 MYSQL_USER                   thresh                   MySQL username
 MYSQL_PASSWORD               password                 MySQL password
 MYSQL_DATABASE               mon                      MySQL database name
 MYSQL_WAIT_RETRIES           24                       Number of MySQL connection attempts
 MYSQL_WAIT_DELAY             5                        Seconds to wait between attempts
 ZOOKEEPER_URL                zookeeper:2181           Zookeeper URL
 NO_STORM_CLUSTER             unset                    If ``true``, run without Storm daemons
 STORM_WAIT_RETRIES           24                       # of tries to verify Storm availability
 STORM_WAIT_DELAY             5                        # seconds between retry attempts
 WORKER_MAX_MB                unset                    If set and ``NO_STORM_CLUSTER``is ``true``,
                                                       use as MaxRam Size for JVM
 METRIC_SPOUT_THREADS         2                        Metric Spout threads
 METRIC_SPOUT_TASKS           2                        Metric Spout tasks
 EVENT_SPOUT_THREADS          2                        Event Spout Threads
 EVENT_SPOUT_TASKS            2                        Event Spout Tasks
 EVENT_BOLT_THREADS           2                        Event Bolt Threads
 EVENT_BOLT_TASKS             2                        Event Bolt Tasks
 FILTERING_BOLT_THREADS       2                        Filtering Bolt Threads
 FILTERING_BOLT_TASKS         2                        Filtering Bolt Tasks
 ALARM_CREATION_BOLT_THREADS  2                        Alarm Creation Bolt Threads
 ALARM_CREATION_BOLT_TASKS    2                        Alarm Creation Bolt Tasks
 AGGREGATION_BOLT_THREADS     2                        Aggregation Bolt Threads
 AGGREGATION_BOLT_TASKS       2                        Aggregation Bolt Tasks
 THRESHOLDING_BOLT_THREADS    2                        Thresholding Bolt Threads
 THRESHOLDING_BOLT_TASKS      2                        Thresholding Bolt Tasks
 THRESH_STACK_SIZE            1024k                    JVM stack size
============================= ======================== ============================================


Wait scripts environment variables
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
======================== ================================ =========================================
Variable                 Default                          Description
======================== ================================ =========================================
KAFKA_URI                kafka:9092                       URI to Apache Kafka
KAFKA_WAIT_FOR_TOPICS    alarm-state-transitions,metrics, Comma-separated list of topic names
                         events                           to check
KAFKA_WAIT_RETRIES       24                               Number of kafka connection attempts
KAFKA_WAIT_DELAY         5                                Seconds to wait between attempts
MYSQL_HOST               mysql                            The host for MySQL
MYSQL_PORT               3306                             The port for MySQL
MYSQL_USER               monapi                           The MySQL username
MYSQL_PASSWORD           password                         The MySQL password
MYSQL_DB                 mon                              The MySQL database name
MYSQL_WAIT_RETRIES       24                               Number of MySQL connection attempts
MYSQL_WAIT_DELAY         5                                Seconds to wait between attempts
======================== ================================ =========================================

Building Monasca Thresh image
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Example:
  $ ./build_image.sh <repository_version> <upper_constains_branch> <common_version>

Everything after ``./build_image.sh`` is optional and by default configured
to get versions from ``Dockerfile``. ``./build_image.sh`` also contain more
detailed build description.


Scripts
~~~~~~~
start.sh
    In this starting script provide all steps that lead to the proper service
    start. Including usage of wait scripts and templating of configuration
    files. You also could provide the ability to allow running container after
    service died for easier debugging.

health_check.py
  This file will be used for checking the status of the application.

# TODO: Test how it's working or if it's working

Running with and without Storm
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The Threshold Engine can be run in two different modes, with Storm Daemons
or without Storm Daemons. If run with the Storm Daemons, multiple
Storm Supervisor containers can be used with more than one worker process
in each. With no Storm Daemons, only a single Threshold Engine container
can be run with a single worker process.

The default docker-compose.yml file is configured to run without Storm.
To change docker-compose.yml to run with Storm, delete the `thresh` service
entry and replace it with the below::

  nimbus:
    image: storm:1.1.1
    environment:
      LOGSTASH_FIELDS: "service=nimbus"
    command: storm nimbus
    depends_on:
      - zookeeper
    links:
      - zookeeper
    restart: unless-stopped

  supervisor:
    image: storm:1.1.1
    environment:
      LOGSTASH_FIELDS: "service=supervisor"
    command: storm supervisor
    depends_on:
      - nimbus
      - zookeeper
    links:
      - nimbus
      - zookeeper
    restart: unless-stopped

  thresh-init:
    image: monasca/thresh:master
    environment:
      NIMBUS_SEEDS: "nimbus"
      WORKER_MAX_HEAP_MB: "256"
      LOGSTASH_FIELDS: "service=monasca-thresh"
    depends_on:
      - zookeeper
      - kafka
      - nimbus
      - supervisor


.. _wiki: https://wiki.openstack.org/wiki/Monasca
.. _monasca-thresh: https://opendev.org/openstack/monasca-thresh
.. _monasca-docker: https://github.com/monasca/monasca-docker/
.. _Dockerfile: https://opendev.org/openstack/monasca-thresh/src/branch/master/docker/Dockerfile
.. _`Storm monasca-api`: https://github.com/monasca/monasca-docker/blob/master/storm/Dockerfile
.. _Kubernetes: https://github.com/monasca/monasca-helm
__ monasca-docker_
