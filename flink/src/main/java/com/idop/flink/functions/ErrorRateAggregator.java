package com.idop.flink.functions;

import com.idop.flink.models.TelemetryEvent;
import com.idop.flink.models.AlertEvent;
import com.idop.flink.models.MLFeaturePayload;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * REQ-2.5: Computes 1-minute error rates and threshold alerts
 * using event-time semantics via stateful tumbling windows.
 *
 * Emits MLFeaturePayload to the main output stream.
 * Emits AlertEvent to the side output when thresholds are breached.
 */
public class ErrorRateAggregator
        extends ProcessWindowFunction<TelemetryEvent, MLFeaturePayload, String, TimeWindow> {

    private static final Logger LOG = LoggerFactory.getLogger(ErrorRateAggregator.class);
    private static final long serialVersionUID = 1L;

    // Error rate threshold for alerts
    private static final double ERROR_RATE_THRESHOLD = 0.1;  // 10%
    private static final int MAX_TRACE_IDS = 50;

    private final OutputTag<AlertEvent> alertTag;

    public ErrorRateAggregator(OutputTag<AlertEvent> alertTag) {
        this.alertTag = alertTag;
    }

    @Override
    public void process(String serviceName,
                        ProcessWindowFunction<TelemetryEvent, MLFeaturePayload, String, TimeWindow>.Context context,
                        Iterable<TelemetryEvent> events,
                        Collector<MLFeaturePayload> out) {

        long totalEvents = 0;
        long errorCount = 0;
        long anomalyCount = 0;
        List<String> traceIds = new ArrayList<>();
        Map<String, Object> aggregatedFeatures = new HashMap<>();

        // Counters for feature aggregation
        double totalLatency = 0;
        long latencyCount = 0;
        long gcEventCount = 0;
        long connectionFailures = 0;
        long securityEvents = 0;

        for (TelemetryEvent event : events) {
            totalEvents++;

            String severity = event.getSeverity() != null ? event.getSeverity().toUpperCase() : "INFO";
            if ("ERROR".equals(severity) || "CRITICAL".equals(severity) || "FATAL".equals(severity)) {
                errorCount++;
            }

            if (event.isAnomalous()) {
                anomalyCount++;
            }

            if (event.isSecurityFlag()) {
                securityEvents++;
            }

            // Collect representative trace IDs
            if (event.getTraceId() != null && traceIds.size() < MAX_TRACE_IDS) {
                traceIds.add(event.getTraceId());
            }

            // Aggregate ML features
            Map<String, Object> features = event.getMlFeatures();
            if (features != null) {
                Object latencyObj = features.get("latency_ms");
                if (latencyObj != null) {
                    totalLatency += ((Number) latencyObj).doubleValue();
                    latencyCount++;
                }
                if (Boolean.TRUE.equals(features.get("has_gc_event"))) {
                    gcEventCount++;
                }
                if (Boolean.TRUE.equals(features.get("connection_failure"))) {
                    connectionFailures++;
                }
            }
        }

        if (totalEvents == 0) return;

        double errorRate = (double) errorCount / totalEvents;

        // Build aggregated features map
        aggregatedFeatures.put("avg_latency_ms", latencyCount > 0 ? totalLatency / latencyCount : 0.0);
        aggregatedFeatures.put("gc_event_count", gcEventCount);
        aggregatedFeatures.put("connection_failures", connectionFailures);
        aggregatedFeatures.put("security_events", securityEvents);

        // Build ML feature payload
        MLFeaturePayload payload = new MLFeaturePayload();
        payload.setServiceName(serviceName);
        payload.setWindowStart(context.window().getStart());
        payload.setWindowEnd(context.window().getEnd());
        payload.setTotalEvents(totalEvents);
        payload.setErrorCount(errorCount);
        payload.setErrorRate(errorRate);
        payload.setAnomalyCount(anomalyCount);
        payload.setTraceIds(traceIds);
        payload.setAggregatedFeatures(aggregatedFeatures);

        out.collect(payload);

        // ── Threshold Alerting ──
        if (errorRate > ERROR_RATE_THRESHOLD) {
            String message = String.format(
                    "Error rate %.2f%% exceeds threshold %.2f%% for service '%s' in window [%d, %d]",
                    errorRate * 100, ERROR_RATE_THRESHOLD * 100, serviceName,
                    context.window().getStart(), context.window().getEnd());

            AlertEvent alert = new AlertEvent(
                    serviceName, "ERROR_RATE_THRESHOLD", message,
                    context.window().getStart(), context.window().getEnd(),
                    errorRate, totalEvents);

            context.output(alertTag, alert);
            LOG.warn("Alert triggered: {}", message);
        }

        // Security alert
        if (securityEvents > 0) {
            String message = String.format(
                    "%d security events detected for service '%s' in window [%d, %d]",
                    securityEvents, serviceName,
                    context.window().getStart(), context.window().getEnd());

            AlertEvent secAlert = new AlertEvent(
                    serviceName, "SECURITY_EVENT", message,
                    context.window().getStart(), context.window().getEnd(),
                    errorRate, totalEvents);

            context.output(alertTag, secAlert);
            LOG.warn("Security alert triggered: {}", message);
        }
    }
}
