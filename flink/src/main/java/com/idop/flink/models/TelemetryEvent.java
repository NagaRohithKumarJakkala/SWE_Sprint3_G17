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

    // Constructors
    public TelemetryEvent() {}

    // Getters & Setters
    public String getTimestamp() { return timestamp; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }

    public String getTraceId() { return traceId; }
    public void setTraceId(String traceId) { this.traceId = traceId; }

    public String getSpanId() { return spanId; }
    public void setSpanId(String spanId) { this.spanId = spanId; }

    public String getServiceName() { return serviceName; }
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
}
