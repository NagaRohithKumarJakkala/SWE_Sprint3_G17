package com.idop.flink.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;

/**
 * Alert event emitted when error rate thresholds are breached.
 * Published to the alerts-critical topic.
 */
public class AlertEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("alert_id")
    private String alertId;

    @JsonProperty("service_name")
    private String serviceName;

    @JsonProperty("alert_type")
    private String alertType;

    @JsonProperty("severity")
    private String severity;

    @JsonProperty("message")
    private String message;

    @JsonProperty("window_start")
    private long windowStart;

    @JsonProperty("window_end")
    private long windowEnd;

    @JsonProperty("error_rate")
    private double errorRate;

    @JsonProperty("event_count")
    private long eventCount;

    @JsonProperty("triggered_at")
    private long triggeredAt;

    public AlertEvent() {}

    public AlertEvent(String serviceName, String alertType, String message,
                      long windowStart, long windowEnd, double errorRate, long eventCount) {
        this.alertId = java.util.UUID.randomUUID().toString();
        this.serviceName = serviceName;
        this.alertType = alertType;
        this.severity = errorRate > 0.5 ? "CRITICAL" : "WARNING";
        this.message = message;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.errorRate = errorRate;
        this.eventCount = eventCount;
        this.triggeredAt = System.currentTimeMillis();
    }

    // Getters
    public String getAlertId() { return alertId; }
    public String getServiceName() { return serviceName; }
    public String getAlertType() { return alertType; }
    public String getSeverity() { return severity; }
    public String getMessage() { return message; }
    public long getWindowStart() { return windowStart; }
    public long getWindowEnd() { return windowEnd; }
    public double getErrorRate() { return errorRate; }
    public long getEventCount() { return eventCount; }
    public long getTriggeredAt() { return triggeredAt; }
}
