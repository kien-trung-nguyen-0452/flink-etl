FROM  flink:1.19-java17

COPY target/flink-etl.jar /opt/flink/usrlib/flink-etl.jar
COPY config /opt/flink/config

WORKDIR /opt/flink