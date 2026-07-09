FROM flink:1.19.0-java21

COPY target/flink-etl.jar /opt/flink/usrlib/flink-etl.jar
COPY config /opt/flink/config

WORKDIR /opt/flink