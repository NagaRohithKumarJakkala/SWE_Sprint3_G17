package com.idop.flink.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Schema B: Enriched ML feature payload for the telemetry-ml-features topic.
 *
 * Represents a 5-minute tumbling window feature vector per (service_name, window).
 * All fields are numeric or fixed-length — ready for XGBoost DMatrix or LSTM tensor.
 *
 * REQ-2.8: Bundles anomalous stream events with fetched ClickHouse context.
 */
public class MLFeaturePayload implements Serializable {

    private static final long serialVersionUID = 2L;

    @JsonProperty("schema_version")
    private String schemaVersion = "ml-features-v2";

    @JsonProperty("payload_id")
    private String payloadId;

    @JsonProperty("service_name")
    private String serviceName;

    @JsonProperty("window_start_epoch_ms")
    private long windowStartEpochMs;

    @JsonProperty("window_end_epoch_ms")
    private long windowEndEpochMs;

    @JsonProperty("window_duration_sec")
    private int windowDurationSec = 300;

    // ── Typed Feature Groups ──

    @JsonProperty("volume_features")
    private VolumeFeatures volumeFeatures = new VolumeFeatures();

    @JsonProperty("rate_features")
    private RateFeatures rateFeatures = new RateFeatures();

    @JsonProperty("latency_features")
    private LatencyFeatures latencyFeatures = new LatencyFeatures();

    @JsonProperty("gc_features")
    private GcFeatures gcFeatures = new GcFeatures();

    @JsonProperty("dependency_features")
    private DependencyFeatures dependencyFeatures = new DependencyFeatures();

    @JsonProperty("distribution_features")
    private DistributionFeatures distributionFeatures = new DistributionFeatures();

    @JsonProperty("category_distribution")
    private Map<String, Double> categoryDistribution = new HashMap<>();

    @JsonProperty("security_features")
    private SecurityFeatures securityFeatures = new SecurityFeatures();

    @JsonProperty("metadata")
    private FeatureMetadata metadata = new FeatureMetadata();

    // Historical context fetched from ClickHouse (REQ-2.7)
    @JsonProperty("historical_context")
    private List<Map<String, Object>> historicalContext;

    public MLFeaturePayload() {
        this.payloadId = java.util.UUID.randomUUID().toString();
        this.metadata.setCreatedAtEpochMs(System.currentTimeMillis());
    }

    // ════════════════════════════════════════════════════════
    // Inner Model Classes
    // ════════════════════════════════════════════════════════

    public static class VolumeFeatures implements Serializable {
        private static final long serialVersionUID = 1L;

        @JsonProperty("event_count")      private long eventCount;
        @JsonProperty("error_count")      private long errorCount;
        @JsonProperty("warn_count")       private long warnCount;
        @JsonProperty("trace_count_distinct") private long traceCountDistinct;

        public long getEventCount() { return eventCount; }
        public void setEventCount(long v) { this.eventCount = v; }
        public long getErrorCount() { return errorCount; }
        public void setErrorCount(long v) { this.errorCount = v; }
        public long getWarnCount() { return warnCount; }
        public void setWarnCount(long v) { this.warnCount = v; }
        public long getTraceCountDistinct() { return traceCountDistinct; }
        public void setTraceCountDistinct(long v) { this.traceCountDistinct = v; }
    }

    public static class RateFeatures implements Serializable {
        private static final long serialVersionUID = 1L;

        @JsonProperty("error_rate")        private double errorRate;
        @JsonProperty("error_velocity")    private double errorVelocity;
        @JsonProperty("stack_trace_ratio") private double stackTraceRatio;
        @JsonProperty("server_error_rate") private double serverErrorRate;

        public double getErrorRate() { return errorRate; }
        public void setErrorRate(double v) { this.errorRate = v; }
        public double getErrorVelocity() { return errorVelocity; }
        public void setErrorVelocity(double v) { this.errorVelocity = v; }
        public double getStackTraceRatio() { return stackTraceRatio; }
        public void setStackTraceRatio(double v) { this.stackTraceRatio = v; }
        public double getServerErrorRate() { return serverErrorRate; }
        public void setServerErrorRate(double v) { this.serverErrorRate = v; }
    }

    public static class LatencyFeatures implements Serializable {
        private static final long serialVersionUID = 1L;

        @JsonProperty("latency_avg_ms")       private double latencyAvgMs;
        @JsonProperty("latency_p50_ms")        private double latencyP50Ms;
        @JsonProperty("latency_p95_ms")        private double latencyP95Ms;
        @JsonProperty("latency_p99_ms")        private double latencyP99Ms;
        @JsonProperty("latency_p99_p50_ratio") private double latencyP99P50Ratio;

        public double getLatencyAvgMs() { return latencyAvgMs; }
        public void setLatencyAvgMs(double v) { this.latencyAvgMs = v; }
        public double getLatencyP50Ms() { return latencyP50Ms; }
        public void setLatencyP50Ms(double v) { this.latencyP50Ms = v; }
        public double getLatencyP95Ms() { return latencyP95Ms; }
        public void setLatencyP95Ms(double v) { this.latencyP95Ms = v; }
        public double getLatencyP99Ms() { return latencyP99Ms; }
        public void setLatencyP99Ms(double v) { this.latencyP99Ms = v; }
        public double getLatencyP99P50Ratio() { return latencyP99P50Ratio; }
        public void setLatencyP99P50Ratio(double v) { this.latencyP99P50Ratio = v; }
    }

    public static class GcFeatures implements Serializable {
        private static final long serialVersionUID = 1L;

        @JsonProperty("gc_event_count") private long gcEventCount;
        @JsonProperty("gc_pause_max_ms") private double gcPauseMaxMs;
        @JsonProperty("gc_pause_avg_ms") private double gcPauseAvgMs;

        public long getGcEventCount() { return gcEventCount; }
        public void setGcEventCount(long v) { this.gcEventCount = v; }
        public double getGcPauseMaxMs() { return gcPauseMaxMs; }
        public void setGcPauseMaxMs(double v) { this.gcPauseMaxMs = v; }
        public double getGcPauseAvgMs() { return gcPauseAvgMs; }
        public void setGcPauseAvgMs(double v) { this.gcPauseAvgMs = v; }
    }

    public static class DependencyFeatures implements Serializable {
        private static final long serialVersionUID = 1L;

        @JsonProperty("connection_failure_count") private long connectionFailureCount;
        @JsonProperty("connection_wait_avg_ms")   private double connectionWaitAvgMs;
        @JsonProperty("retry_total")              private long retryTotal;

        public long getConnectionFailureCount() { return connectionFailureCount; }
        public void setConnectionFailureCount(long v) { this.connectionFailureCount = v; }
        public double getConnectionWaitAvgMs() { return connectionWaitAvgMs; }
        public void setConnectionWaitAvgMs(double v) { this.connectionWaitAvgMs = v; }
        public long getRetryTotal() { return retryTotal; }
        public void setRetryTotal(long v) { this.retryTotal = v; }
    }

    public static class DistributionFeatures implements Serializable {
        private static final long serialVersionUID = 1L;

        @JsonProperty("severity_entropy")           private double severityEntropy;
        @JsonProperty("exception_type_cardinality") private int exceptionTypeCardinality;
        @JsonProperty("log_body_size_avg_bytes")    private double logBodySizeAvgBytes;
        @JsonProperty("log_body_size_p95_bytes")    private double logBodySizeP95Bytes;

        public double getSeverityEntropy() { return severityEntropy; }
        public void setSeverityEntropy(double v) { this.severityEntropy = v; }
        public int getExceptionTypeCardinality() { return exceptionTypeCardinality; }
        public void setExceptionTypeCardinality(int v) { this.exceptionTypeCardinality = v; }
        public double getLogBodySizeAvgBytes() { return logBodySizeAvgBytes; }
        public void setLogBodySizeAvgBytes(double v) { this.logBodySizeAvgBytes = v; }
        public double getLogBodySizeP95Bytes() { return logBodySizeP95Bytes; }
        public void setLogBodySizeP95Bytes(double v) { this.logBodySizeP95Bytes = v; }
    }

    public static class SecurityFeatures implements Serializable {
        private static final long serialVersionUID = 1L;

        @JsonProperty("security_event_count") private long securityEventCount;

        public long getSecurityEventCount() { return securityEventCount; }
        public void setSecurityEventCount(long v) { this.securityEventCount = v; }
    }

    public static class FeatureMetadata implements Serializable {
        private static final long serialVersionUID = 1L;

        @JsonProperty("anomaly_count")             private long anomalyCount;
        @JsonProperty("representative_trace_ids")   private List<String> representativeTraceIds;
        @JsonProperty("created_at_epoch_ms")         private long createdAtEpochMs;

        public long getAnomalyCount() { return anomalyCount; }
        public void setAnomalyCount(long v) { this.anomalyCount = v; }
        public List<String> getRepresentativeTraceIds() { return representativeTraceIds; }
        public void setRepresentativeTraceIds(List<String> v) { this.representativeTraceIds = v; }
        public long getCreatedAtEpochMs() { return createdAtEpochMs; }
        public void setCreatedAtEpochMs(long v) { this.createdAtEpochMs = v; }
    }

    // ════════════════════════════════════════════════════════
    // Top-Level Getters & Setters
    // ════════════════════════════════════════════════════════

    public String getSchemaVersion() { return schemaVersion; }
    public String getPayloadId() { return payloadId; }

    public String getServiceName() { return serviceName; }
    public void setServiceName(String serviceName) { this.serviceName = serviceName; }

    public long getWindowStartEpochMs() { return windowStartEpochMs; }
    public void setWindowStartEpochMs(long v) { this.windowStartEpochMs = v; }

    public long getWindowEndEpochMs() { return windowEndEpochMs; }
    public void setWindowEndEpochMs(long v) { this.windowEndEpochMs = v; }

    public int getWindowDurationSec() { return windowDurationSec; }
    public void setWindowDurationSec(int v) { this.windowDurationSec = v; }

    public VolumeFeatures getVolumeFeatures() { return volumeFeatures; }
    public void setVolumeFeatures(VolumeFeatures v) { this.volumeFeatures = v; }

    public RateFeatures getRateFeatures() { return rateFeatures; }
    public void setRateFeatures(RateFeatures v) { this.rateFeatures = v; }

    public LatencyFeatures getLatencyFeatures() { return latencyFeatures; }
    public void setLatencyFeatures(LatencyFeatures v) { this.latencyFeatures = v; }

    public GcFeatures getGcFeatures() { return gcFeatures; }
    public void setGcFeatures(GcFeatures v) { this.gcFeatures = v; }

    public DependencyFeatures getDependencyFeatures() { return dependencyFeatures; }
    public void setDependencyFeatures(DependencyFeatures v) { this.dependencyFeatures = v; }

    public DistributionFeatures getDistributionFeatures() { return distributionFeatures; }
    public void setDistributionFeatures(DistributionFeatures v) { this.distributionFeatures = v; }

    public Map<String, Double> getCategoryDistribution() { return categoryDistribution; }
    public void setCategoryDistribution(Map<String, Double> v) { this.categoryDistribution = v; }

    public SecurityFeatures getSecurityFeatures() { return securityFeatures; }
    public void setSecurityFeatures(SecurityFeatures v) { this.securityFeatures = v; }

    public FeatureMetadata getMetadata() { return metadata; }
    public void setMetadata(FeatureMetadata v) { this.metadata = v; }

    public List<Map<String, Object>> getHistoricalContext() { return historicalContext; }
    public void setHistoricalContext(List<Map<String, Object>> v) { this.historicalContext = v; }
}
