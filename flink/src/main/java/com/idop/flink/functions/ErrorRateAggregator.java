package com.idop.flink.functions;

import com.idop.flink.models.TelemetryEvent;
import com.idop.flink.models.AlertEvent;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * REQ-2.5: 1-Minute Tumbling Window — Alerts ONLY.
 *
 * Computes basic error rates for threshold alerting with low latency.
 * All ML feature computation is handled by the parallel 5-minute
 * MLFeatureAggregator window — this class focuses solely on fast
 * alert generation.
 *
 * Emits AlertEvent via side output when thresholds are breached.
 */
public class ErrorRateAggregator
        extends ProcessWindowFunction<TelemetryEvent, AlertEvent, String, TimeWindow> {

    private static final Logger LOG = LoggerFactory.getLogger(ErrorRateAggregator.class);
    private static final long serialVersionUID = 2L;

    // Thresholds
    private static final double ERROR_RATE_THRESHOLD = 0.10;   // 10%
    private static final long CONNECTION_FAILURE_THRESHOLD = 5;
    private static final long GC_EVENT_THRESHOLD = 10;

    @Override
    public void process(String serviceName,
                        ProcessWindowFunction<TelemetryEvent, AlertEvent, String, TimeWindow>.Context context,
                        Iterable<TelemetryEvent> events,
                        Collector<AlertEvent> out) {

        long totalEvents = 0;
        long errorCount = 0;
        long securityEvents = 0;
        long connectionFailures = 0;
        long gcEvents = 0;

        for (TelemetryEvent event : events) {
            totalEvents++;

            String severity = event.getSeverity() != null ? event.getSeverity().toUpperCase() : "INFO";
            if ("ERROR".equals(severity) || "CRITICAL".equals(severity) || "FATAL".equals(severity)) {
                errorCount++;
            }

            if (event.isSecurityFlag()) {
                securityEvents++;
            }

            if (event.getMlFeatureBool("connection_failure")) {
                connectionFailures++;
            }
            if (event.getMlFeatureBool("has_gc_event")) {
                gcEvents++;
            }
        }

        if (totalEvents == 0) return;

        double errorRate = (double) errorCount / totalEvents;

        // ── Error Rate Alert ──
        if (errorRate > ERROR_RATE_THRESHOLD) {
            String message = String.format(
                    "Error rate %.2f%% exceeds threshold %.2f%% for service '%s' in window [%d, %d]",
                    errorRate * 100, ERROR_RATE_THRESHOLD * 100, serviceName,
                    context.window().getStart(), context.window().getEnd());

            out.collect(new AlertEvent(
                    serviceName, "ERROR_RATE_THRESHOLD", message,
                    context.window().getStart(), context.window().getEnd(),
                    errorRate, totalEvents));

            LOG.warn("Alert triggered: {}", message);
        }

        // ── Security Alert ──
        if (securityEvents > 0) {
            String message = String.format(
                    "%d security events detected for service '%s' in window [%d, %d]",
                    securityEvents, serviceName,
                    context.window().getStart(), context.window().getEnd());

            out.collect(new AlertEvent(
                    serviceName, "SECURITY_EVENT", message,
                    context.window().getStart(), context.window().getEnd(),
                    errorRate, totalEvents));

            LOG.warn("Security alert: {}", message);
        }

        // ── Connection Failure Storm Alert ──
        if (connectionFailures >= CONNECTION_FAILURE_THRESHOLD) {
            String message = String.format(
                    "%d connection failures for service '%s' in window [%d, %d]",
                    connectionFailures, serviceName,
                    context.window().getStart(), context.window().getEnd());

            out.collect(new AlertEvent(
                    serviceName, "CONNECTION_FAILURE_STORM", message,
                    context.window().getStart(), context.window().getEnd(),
                    errorRate, totalEvents));

            LOG.warn("Connection storm alert: {}", message);
        }

        // ── GC Pressure Alert ──
        if (gcEvents >= GC_EVENT_THRESHOLD) {
            String message = String.format(
                    "%d GC events for service '%s' in window [%d, %d]",
                    gcEvents, serviceName,
                    context.window().getStart(), context.window().getEnd());

            out.collect(new AlertEvent(
                    serviceName, "GC_PRESSURE", message,
                    context.window().getStart(), context.window().getEnd(),
                    errorRate, totalEvents));

            LOG.warn("GC pressure alert: {}", message);
        }
    }
}
