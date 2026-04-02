package com.idop.flink.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.Map;

/**
 * Represents a telemetry event from the telemetry-hot topic.
 * Matches the schema produced by the Vector Aggregator.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TelemetryEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("timestamp")
    private String timestamp;

    @JsonProperty("trace_id")
    private String traceId;

    @JsonProperty("span_id")
    private String spanId;

    @JsonProperty("service_name")
    private String serviceName;

    @JsonProperty("severity")
    private String severity;

    @JsonProperty("body")
    private String body;

    @JsonProperty("ml_features")
    private Map<String, Object> mlFeatures;

    @JsonProperty("is_anomalous")
    private boolean isAnomalous;

    @JsonProperty("anomaly_reason")
    private String anomalyReason;

    @JsonProperty("is_metric")
    private boolean isMetric;

    @JsonProperty("is_security_flag")
    private boolean isSecurityFlag;

    @JsonProperty("resource_flat")
    private Map<String, Object> resourceFlat;

    // Constructors
    public TelemetryEvent() {}

    // Getters & Setters
    public String getTimestamp() { return timestamp; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }

    public String getTraceId() { return traceId; }
    public void setTraceId(String traceId) { this.traceId = traceId; }

    public String getSpanId() { return spanId; }
    public void setSpanId(String spanId) { this.spanId = spanId; }

    public String getServiceName() {
        if (serviceName != null) return serviceName;
        return "unknown";
    }
    public void setServiceName(String serviceName) { this.serviceName = serviceName; }

    public String getSeverity() { return severity; }
    public void setSeverity(String severity) { this.severity = severity; }

    public String getBody() { return body; }
    public void setBody(String body) { this.body = body; }

    public Map<String, Object> getMlFeatures() { return mlFeatures; }
    public void setMlFeatures(Map<String, Object> mlFeatures) { this.mlFeatures = mlFeatures; }

    public boolean isAnomalous() { return isAnomalous; }
    public void setAnomalous(boolean anomalous) { isAnomalous = anomalous; }

    public String getAnomalyReason() { return anomalyReason; }
    public void setAnomalyReason(String anomalyReason) { this.anomalyReason = anomalyReason; }

    public boolean isMetric() { return isMetric; }
    public void setMetric(boolean metric) { isMetric = metric; }

    public boolean isSecurityFlag() { return isSecurityFlag; }
    public void setSecurityFlag(boolean securityFlag) { isSecurityFlag = securityFlag; }

    public Map<String, Object> getResourceFlat() { return resourceFlat; }
    public void setResourceFlat(Map<String, Object> resourceFlat) { this.resourceFlat = resourceFlat; }

    // ── ML Feature Accessors (convenience) ──

    /** Safely extract a double from the ml_features map. */
    public double getMlFeatureDouble(String key) {
        if (mlFeatures == null) return 0.0;
        Object val = mlFeatures.get(key);
        if (val instanceof Number) return ((Number) val).doubleValue();
        return 0.0;
    }

    /** Safely extract a boolean from the ml_features map. */
    public boolean getMlFeatureBool(String key) {
        if (mlFeatures == null) return false;
        return Boolean.TRUE.equals(mlFeatures.get(key));
    }

    /** Safely extract a string from the ml_features map. */
    public String getMlFeatureString(String key) {
        if (mlFeatures == null) return "";
        Object val = mlFeatures.get(key);
        return val != null ? val.toString() : "";
    }

    /** Safely extract an int from the ml_features map. */
    public int getMlFeatureInt(String key) {
        if (mlFeatures == null) return 0;
        Object val = mlFeatures.get(key);
        if (val instanceof Number) return ((Number) val).intValue();
        return 0;
    }
}
