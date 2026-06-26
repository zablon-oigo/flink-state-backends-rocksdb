ARG FLINK_VERSION=2.0

FROM apache/flink:${FLINK_VERSION}-java21

WORKDIR /opt/flink

COPY target/flink-filter.jar /opt/flink/usrlib/

USER root

RUN mkdir -p \
      /tmp/flink-checkpoints \
      /tmp/flink-savepoints \
      /tmp/flink-rocksdb \
 && chmod -R 777 /tmp/flink-checkpoints \
 && chmod -R 777 /tmp/flink-savepoints \
 && chmod -R 777 /tmp/flink-rocksdb

USER flink