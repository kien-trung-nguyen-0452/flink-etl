package vn.softdreams.flink.repository;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vn.softdreams.flink.repository.configs.ConfigLoader;
import vn.softdreams.flink.repository.configs.JobConfig;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
public class RepositoryLedgerJob {

    private static final Logger LOG = LoggerFactory.getLogger(RepositoryLedgerJob.class);

    public static void main(String[] args) throws Exception {

        JobConfig config = ConfigLoader.load("config/repository-ledger.yml");

        LOG.info(
                "Starting RepositoryLedgerJob | topic={} cluster={}",
                config.kafka.topic,
                config.cluster.id
        );

        // ---------------------------------------------------------------------
        // Flink Environment
        // ---------------------------------------------------------------------
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(config.flink.parallelism);

        EmbeddedRocksDBStateBackend rocksDB = new EmbeddedRocksDBStateBackend(true);
        env.setStateBackend(rocksDB);

        env.enableCheckpointing(
                config.checkpoint.intervalMs,
                CheckpointingMode.EXACTLY_ONCE
        );

        CheckpointConfig ckpConfig = env.getCheckpointConfig();
        ckpConfig.setMinPauseBetweenCheckpoints(config.checkpoint.minPauseMs);
        ckpConfig.setCheckpointTimeout(config.checkpoint.timeoutMs);
        ckpConfig.setMaxConcurrentCheckpoints(1);
        ckpConfig.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );

        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(
                        3,
                        Time.of(10, TimeUnit.SECONDS)
                )
        );

        // ---------------------------------------------------------------------
        // Kafka Source
        // ---------------------------------------------------------------------
        KafkaSource<RepositoryLedgerRecord> kafkaSource = KafkaSource
                .<RepositoryLedgerRecord>builder()
                .setBootstrapServers(config.kafka.brokers)
                .setTopics(config.kafka.topic)
                .setGroupId(config.kafka.group)
                .setStartingOffsets(
                        OffsetsInitializer.committedOffsets(
                                OffsetsInitializer.earliest()
                                        .getAutoOffsetResetStrategy()
                        )
                )
                .setValueOnlyDeserializer(
                        new RepositoryLedgerDeserializer(config.cluster.id)
                )
                .setProperty(
                        "partition.discovery.interval.ms",
                        String.valueOf(config.kafkaSource.partitionDiscoveryMs)
                )
                .setProperty(
                        "max.poll.records",
                        String.valueOf(config.kafkaSource.maxPollRecords)
                )
                .setProperty(
                        "fetch.min.bytes",
                        String.valueOf(config.kafkaSource.fetchMinBytes)
                )
                .setProperty(
                        "fetch.max.wait.ms",
                        String.valueOf(config.kafkaSource.fetchMaxWaitMs)
                )
                .build();

        // ---------------------------------------------------------------------
        // Pipeline
        // ---------------------------------------------------------------------
        DataStream<RepositoryLedgerRecord> stream = env
                .fromSource(
                        kafkaSource,
                        WatermarkStrategy.noWatermarks(),
                        "Kafka-RepositoryLedger"
                )
                .filter(Objects::nonNull)
                .name("filter-null");

        stream.addSink(
                        new ClickHouseSink(
                                config.clickhouse.host,
                                config.clickhouse.port,
                                config.clickhouse.database,
                                config.clickhouse.user,
                                config.clickhouse.password,
                                config.clickhouse.table,
                                config.sink.batchSize,
                                config.sink.flushIntervalMs
                        )
                )
                .name("ClickHouse-RepositoryLedger")
                .setParallelism(config.flink.parallelism);

        env.execute(
                "RepositoryLedger CDC → ClickHouse [" + config.cluster.id + "]"
        );
    }
}