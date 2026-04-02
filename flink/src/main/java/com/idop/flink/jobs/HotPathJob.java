package com.idop.flink.jobs;

import com.idop.flink.functions.ErrorRateAggregator;
import com.idop.flink.functions.MLFeatureAggregator;
import com.idop.flink.functions.AnomalyContextEnricher;
import com.idop.flink.functions.TelemetryTimestampExtractor;
import com.idop.flink.models.TelemetryEvent;
import com.idop.flink.models.AlertEvent;
import com.idop.flink.models.MLFeaturePayload;
import com.idop.flink.serialization.TelemetryDeserializer;
import com.idop.flink.serialization.MLFeatureSerializer;
import com.idop.flink.serialization.AlertSerializer;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Hot Path Streaming Job — Dual Window Architecture.
 *
 * The single telemetry-hot source fans out into two parallel window streams:
 *
 *   1. ALERT PATH  (1-minute tumbling windows)
 *      → ErrorRateAggregator → alerts-critical topic
 *      Purpose: Low-latency threshold alerting (PagerDuty / AlertManager)
 *
 *   2. ML PATH     (5-minute tumbling windows)
 *      → MLFeatureAggregator → AnomalyContextEnricher → telemetry-ml-features topic
 *      Purpose: 40-dimensional feature vectors for XGBoost/LSTM inference
 *
 * REQ-2.5:  Event-time tumbling windows for error rate alerting
 * REQ-2.7:  ClickHouse context enrichment for anomalous windows
 * REQ-2.8:  Enriched ML feature payload to telemetry-ml-features
 * REQ-2.10: Event-time watermarking with 60-second grace period
 */
public class HotPathJob {

    private static final Logger LOG = LoggerFactory.getLogger(HotPathJob.class);

    public static void run(ParameterTool params) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Configuration
        String brokers = params.get("brokers", "redpanda:9092");
        String hotTopic = params.get("hot-topic", "telemetry-hot");
        String mlFeaturesTopic = params.get("ml-features-topic", "telemetry-ml-features");
        String alertsTopic = params.get("alerts-topic", "alerts-critical");
        String clickhouseUrl = params.get("clickhouse-url", "jdbc:clickhouse://clickhouse:8123/telemetry");
        int parallelism = params.getInt("parallelism", 2);

        env.setParallelism(parallelism);
        env.getConfig().setGlobalJobParameters(params);

        // ════════════════════════════════════════════════════
        // Kafka Source (shared by both window paths)
        // ════════════════════════════════════════════════════
        KafkaSource<TelemetryEvent> source = KafkaSource.<TelemetryEvent>builder()
                .setBootstrapServers(brokers)
                .setTopics(hotTopic)
                .setGroupId("flink-hot-consumer")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new TelemetryDeserializer())
                .build();

        // REQ-2.10: Event-time watermarking with 60-second grace period
        WatermarkStrategy<TelemetryEvent> watermarkStrategy = WatermarkStrategy
                .<TelemetryEvent>forBoundedOutOfOrderness(Duration.ofSeconds(60))
                .withTimestampAssigner(new TelemetryTimestampExtractor())
                .withIdleness(Duration.ofMinutes(2));

        DataStream<TelemetryEvent> hotStream = env
                .fromSource(source, watermarkStrategy, "telemetry-hot-source");

        // ════════════════════════════════════════════════════
        // PATH 1: Alert Stream (1-minute tumbling windows)
        // Fast path — produces AlertEvent directly
        // ════════════════════════════════════════════════════
        DataStream<AlertEvent> alerts = hotStream
                .keyBy(TelemetryEvent::getServiceName)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .process(new ErrorRateAggregator())
                .name("1min-alert-aggregator");

        // ════════════════════════════════════════════════════
        // PATH 2: ML Feature Stream (5-minute tumbling windows)
        // Produces 40-dimensional feature vectors for inference
        // ════════════════════════════════════════════════════
        DataStream<MLFeaturePayload> mlFeatures = hotStream
                .keyBy(TelemetryEvent::getServiceName)
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .process(new MLFeatureAggregator())
                .name("5min-ml-feature-aggregator");

        // REQ-2.7: Enrich anomalous windows with ClickHouse historical context
        DataStream<MLFeaturePayload> enriched = mlFeatures
                .keyBy(MLFeaturePayload::getServiceName)
                .process(new AnomalyContextEnricher(clickhouseUrl))
                .name("anomaly-context-enricher");

        // ════════════════════════════════════════════════════
        // Kafka Sink: ML Features → telemetry-ml-features
        // ════════════════════════════════════════════════════
        KafkaSink<MLFeaturePayload> mlFeaturesSink = KafkaSink.<MLFeaturePayload>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(mlFeaturesTopic)
                                .setValueSerializationSchema(new MLFeatureSerializer())
                                .build()
                )
                .build();

        enriched.sinkTo(mlFeaturesSink).name("ml-features-sink");

        // ════════════════════════════════════════════════════
        // Kafka Sink: Alerts → alerts-critical
        // ════════════════════════════════════════════════════
        KafkaSink<AlertEvent> alertsSink = KafkaSink.<AlertEvent>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(alertsTopic)
                                .setValueSerializationSchema(new AlertSerializer())
                                .build()
                )
                .build();

        alerts.sinkTo(alertsSink).name("alerts-sink");

        LOG.info("IDOP Hot Path Job configured: 1-min alerts + 5-min ML features. Starting...");
        env.execute("IDOP Hot Path Pipeline");
    }
}
