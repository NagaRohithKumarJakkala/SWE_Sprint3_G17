package com.idop.flink.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Enriched ML feature payload published to telemetry-ml-features topic.
 * REQ-2.8: Bundles anomalous stream events with fetched ClickHouse context.
 */
public class MLFeaturePayload implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("payload_id")
    private String payloadId;

    @JsonProperty("service_name")
    private String serviceName;

    @JsonProperty("window_start")
    private long windowStart;

    @JsonProperty("window_end")
    private long windowEnd;

    // Aggregated metrics from the tumbling window
    @JsonProperty("total_events")
    private long totalEvents;

    @JsonProperty("error_count")
    private long errorCount;

    @JsonProperty("error_rate")
    private double errorRate;

    @JsonProperty("anomaly_count")
    private long anomalyCount;

    // Representative trace IDs from the window
    @JsonProperty("trace_ids")
    private List<String> traceIds;

    // Aggregated ML features
    @JsonProperty("aggregated_features")
    private Map<String, Object> aggregatedFeatures;

    // Historical context fetched from ClickHouse (REQ-2.7)
    @JsonProperty("historical_context")
    private List<Map<String, Object>> historicalContext;

    @JsonProperty("created_at")
    private long createdAt;

    public MLFeaturePayload() {
        this.payloadId = java.util.UUID.randomUUID().toString();
        this.createdAt = System.currentTimeMillis();
    }

    // Getters & Setters
    public String getPayloadId() { return payloadId; }

    public String getServiceName() { return serviceName; }
    public void setServiceName(String serviceName) { this.serviceName = serviceName; }

    public long getWindowStart() { return windowStart; }
    public void setWindowStart(long windowStart) { this.windowStart = windowStart; }

    public long getWindowEnd() { return windowEnd; }
    public void setWindowEnd(long windowEnd) { this.windowEnd = windowEnd; }

    public long getTotalEvents() { return totalEvents; }
    public void setTotalEvents(long totalEvents) { this.totalEvents = totalEvents; }

    public long getErrorCount() { return errorCount; }
    public void setErrorCount(long errorCount) { this.errorCount = errorCount; }

    public double getErrorRate() { return errorRate; }
    public void setErrorRate(double errorRate) { this.errorRate = errorRate; }

    public long getAnomalyCount() { return anomalyCount; }
    public void setAnomalyCount(long anomalyCount) { this.anomalyCount = anomalyCount; }

    public List<String> getTraceIds() { return traceIds; }
    public void setTraceIds(List<String> traceIds) { this.traceIds = traceIds; }

    public Map<String, Object> getAggregatedFeatures() { return aggregatedFeatures; }
    public void setAggregatedFeatures(Map<String, Object> aggregatedFeatures) { this.aggregatedFeatures = aggregatedFeatures; }

    public List<Map<String, Object>> getHistoricalContext() { return historicalContext; }
    public void setHistoricalContext(List<Map<String, Object>> historicalContext) { this.historicalContext = historicalContext; }

    public long getCreatedAt() { return createdAt; }
}
