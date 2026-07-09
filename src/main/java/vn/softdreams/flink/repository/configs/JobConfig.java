package vn.softdreams.flink.repository.configs;

public class JobConfig {
    public Kafka       kafka;
    public Cluster     cluster;
    public ClickHouse  clickhouse;
    public Flink       flink;
    public Checkpoint  checkpoint;
    public Sink        sink;
    public KafkaSourceConfig kafkaSource;
    public static class Kafka {
        public String brokers;
        public String topic;
        public String group;
    }
    public static class Cluster {
        public String id;
    }
    public static class ClickHouse {
        public String host;
        public int    port;
        public String database;
        public String user;
        public String password;
        public String table;
    }
    public static class Flink {
        public int parallelism;
    }
    public static class Checkpoint {
        public long intervalMs;
        public long minPauseMs;
        public long timeoutMs;
    }
    public static class Sink {
        public int  batchSize;
        public long flushIntervalMs;
    }
    public static class KafkaSourceConfig {
        public long partitionDiscoveryMs;
        public int  maxPollRecords;
        public int  fetchMinBytes;
        public int  fetchMaxWaitMs;
    }
}