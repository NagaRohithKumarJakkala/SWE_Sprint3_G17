package com.idop.flink.functions;

import com.idop.flink.models.TelemetryEvent;
import com.idop.flink.models.MLFeaturePayload;
import com.idop.flink.models.MLFeaturePayload.*;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 5-Minute Tumbling Window ML Feature Aggregator.
 *
 * Computes the full 40-dimensional feature vector per (service_name, window):
 *   - Volume features (4): event_count, error_count, warn_count, trace_count_distinct
 *   - Rate features (4): error_rate, error_velocity, stack_trace_ratio, server_error_rate
 *   - Latency features (5): avg, P50, P95, P99, P99/P50 ratio
 *   - GC features (3): gc_event_count, gc_pause_max, gc_pause_avg
 *   - Dependency features (3): connection_failure_count, connection_wait_avg, retry_total
 *   - Distribution features (4): severity_entropy, exception_type_cardinality,
 *                                  log_body_size_avg, log_body_size_p95
 *   - Category distribution (15): normalized frequency of each log_body_category
 *   - Security features (1): security_event_count
 *   - Metadata: anomaly_count, representative trace IDs
 *
 * Uses globalState() to maintain previous-window error_rate for velocity computation.
 */
public class MLFeatureAggregator
        extends ProcessWindowFunction<TelemetryEvent, MLFeaturePayload, String, TimeWindow> {

    private static final Logger LOG = LoggerFactory.getLogger(MLFeatureAggregator.class);
    private static final long serialVersionUID = 1L;

    private static final int MAX_TRACE_IDS = 50;

    /** The 15 closed log body categories (must match VRL waterfall). */
    private static final String[] CATEGORIES = {
            "gc_event", "oom_error", "connection_timeout", "connection_refused",
            "connection_pool_wait", "db_query_error", "auth_failure", "permission_denied",
            "http_client_error", "http_server_error", "thread_pool_exhaustion",
            "circuit_breaker_open", "retry_exhausted", "stack_trace",
            "startup_shutdown", "health_check", "normal_operation"
    };

    @Override
    public void process(String serviceName,
                        ProcessWindowFunction<TelemetryEvent, MLFeaturePayload, String, TimeWindow>.Context context,
                        Iterable<TelemetryEvent> events,
                        Collector<MLFeaturePayload> out) throws Exception {

        // ────────────────────────────────────────────────
        // Accumulators
        // ────────────────────────────────────────────────
        long totalEvents = 0;
        long errorCount = 0;
        long warnCount = 0;
        long anomalyCount = 0;
        long securityEvents = 0;
        long gcEventCount = 0;
        long connectionFailures = 0;
        long serverErrorCount = 0;
        long stackTraceCount = 0;
        long retryTotal = 0;

        // Severity counts: 0=TRACE, 1=DEBUG, 2=INFO, 3=WARN, 4=ERROR, 5=FATAL
        double[] severityCounts = new double[6];

        // Latency samples (for percentile computation)
        List<Double> latencySamples = new ArrayList<>();

        // GC pause samples
        List<Double> gcPauseSamples = new ArrayList<>();

        // Connection wait samples
        List<Double> connectionWaitSamples = new ArrayList<>();

        // Log body size samples
        List<Double> logBodySizeSamples = new ArrayList<>();

        // Category counters
        Map<String, Long> categoryCounts = new LinkedHashMap<>();
        for (String cat : CATEGORIES) {
            categoryCounts.put(cat, 0L);
        }

        // Distinct exception types
        Set<String> exceptionTypes = new HashSet<>();

        // Distinct trace IDs
        Set<String> distinctTraceIds = new HashSet<>();
        List<String> representativeTraceIds = new ArrayList<>();

        // ────────────────────────────────────────────────
        // Single pass over all events in the window
        // ────────────────────────────────────────────────
        for (TelemetryEvent event : events) {
            totalEvents++;

            // ── Severity ──
            String severity = event.getSeverity() != null ? event.getSeverity().toUpperCase() : "INFO";
            int sevNum = event.getMlFeatureInt("severity_num");
            if (sevNum >= 0 && sevNum < 6) {
                severityCounts[sevNum]++;
            }
            if ("ERROR".equals(severity) || "CRITICAL".equals(severity) || "FATAL".equals(severity)) {
                errorCount++;
            }
            if ("WARN".equals(severity) || "WARNING".equals(severity)) {
                warnCount++;
            }

            // ── Anomaly & Security ──
            if (event.isAnomalous()) anomalyCount++;
            if (event.isSecurityFlag()) securityEvents++;

            // ── Trace IDs ──
            if (event.getTraceId() != null) {
                distinctTraceIds.add(event.getTraceId());
                if (representativeTraceIds.size() < MAX_TRACE_IDS) {
                    representativeTraceIds.add(event.getTraceId());
                }
            }

            // ── ML Features from Vector ──
            Map<String, Object> features = event.getMlFeatures();
            if (features == null) continue;

            // Latency
            double latency = event.getMlFeatureDouble("latency_ms");
            double spanDuration = event.getMlFeatureDouble("span_duration_ms");
            double effectiveLatency = spanDuration > 0 ? spanDuration : latency;
            if (effectiveLatency > 0) {
                latencySamples.add(effectiveLatency);
            }

            // GC
            if (event.getMlFeatureBool("has_gc_event")) {
                gcEventCount++;
                double gcPause = event.getMlFeatureDouble("gc_pause_ms");
                if (gcPause > 0) gcPauseSamples.add(gcPause);
            }

            // Connection failures
            if (event.getMlFeatureBool("connection_failure")) {
                connectionFailures++;
            }

            // Connection wait
            double connWait = event.getMlFeatureDouble("connection_wait_ms");
            if (connWait > 0) connectionWaitSamples.add(connWait);

            // Server errors (5xx)
            String httpClass = event.getMlFeatureString("http_status_class");
            if ("5xx".equals(httpClass)) serverErrorCount++;

            // Stack trace
            if (event.getMlFeatureBool("has_stack_trace")) stackTraceCount++;

            // Retry
            retryTotal += event.getMlFeatureInt("retry_attempt");

            // Log body size
            int bodySize = event.getMlFeatureInt("log_body_length_bytes");
            if (bodySize > 0) logBodySizeSamples.add((double) bodySize);

            // Log body category
            String category = event.getMlFeatureString("log_body_category");
            if (category.isEmpty()) category = "normal_operation";
            categoryCounts.merge(category, 1L, Long::sum);

            // Exception types
            String exType = event.getMlFeatureString("exception_type");
            if (!exType.isEmpty()) exceptionTypes.add(exType);
        }

        if (totalEvents == 0) return;

        // ────────────────────────────────────────────────
        // Compute derived features
        // ────────────────────────────────────────────────
        double errorRate = (double) errorCount / totalEvents;

        // ── Error Velocity (cross-window derivative) ──
        ValueStateDescriptor<Double> prevErrorRateDesc =
                new ValueStateDescriptor<>("prev-error-rate", Types.DOUBLE);
        ValueState<Double> prevErrorRateState = context.globalState().getState(prevErrorRateDesc);

        Double prevErrorRate = prevErrorRateState.value();
        double errorVelocity = 0.0;
        if (prevErrorRate != null) {
            // Δ(error_rate) / Δt, where Δt = window_duration_sec
            long windowDurationSec = (context.window().getEnd() - context.window().getStart()) / 1000;
            if (windowDurationSec > 0) {
                errorVelocity = (errorRate - prevErrorRate) / windowDurationSec;
            }
        }
        prevErrorRateState.update(errorRate);

        // ── Severity Entropy ──
        double severityEntropy = computeEntropy(severityCounts);

        // ── Latency Percentiles ──
        Collections.sort(latencySamples);
        double latencyAvg = average(latencySamples);
        double latencyP50 = percentile(latencySamples, 0.50);
        double latencyP95 = percentile(latencySamples, 0.95);
        double latencyP99 = percentile(latencySamples, 0.99);
        double latencyP99P50Ratio = latencyP50 > 0 ? latencyP99 / latencyP50 : 0.0;

        // ── GC aggregates ──
        double gcPauseMax = gcPauseSamples.isEmpty() ? 0.0
                : gcPauseSamples.stream().mapToDouble(Double::doubleValue).max().orElse(0.0);
        double gcPauseAvg = average(gcPauseSamples);

        // ── Connection wait avg ──
        double connectionWaitAvg = average(connectionWaitSamples);

        // ── Log body size aggregates ──
        Collections.sort(logBodySizeSamples);
        double logBodySizeAvg = average(logBodySizeSamples);
        double logBodySizeP95 = percentile(logBodySizeSamples, 0.95);

        // ── Category distribution (normalized) ──
        Map<String, Double> categoryDistribution = new LinkedHashMap<>();
        for (String cat : CATEGORIES) {
            long count = categoryCounts.getOrDefault(cat, 0L);
            categoryDistribution.put(cat, (double) count / totalEvents);
        }

        // Server error rate
        double serverErrorRate = (double) serverErrorCount / totalEvents;

        // Stack trace ratio
        double stackTraceRatio = (double) stackTraceCount / totalEvents;

        // ────────────────────────────────────────────────
        // Build the typed MLFeaturePayload (Schema B)
        // ────────────────────────────────────────────────
        MLFeaturePayload payload = new MLFeaturePayload();
        payload.setServiceName(serviceName);
        payload.setWindowStartEpochMs(context.window().getStart());
        payload.setWindowEndEpochMs(context.window().getEnd());
        long windowDurSec = (context.window().getEnd() - context.window().getStart()) / 1000;
        payload.setWindowDurationSec((int) windowDurSec);

        // Volume
        VolumeFeatures vol = payload.getVolumeFeatures();
        vol.setEventCount(totalEvents);
        vol.setErrorCount(errorCount);
        vol.setWarnCount(warnCount);
        vol.setTraceCountDistinct(distinctTraceIds.size());

        // Rates
        RateFeatures rates = payload.getRateFeatures();
        rates.setErrorRate(errorRate);
        rates.setErrorVelocity(errorVelocity);
        rates.setStackTraceRatio(stackTraceRatio);
        rates.setServerErrorRate(serverErrorRate);

        // Latency
        LatencyFeatures lat = payload.getLatencyFeatures();
        lat.setLatencyAvgMs(latencyAvg);
        lat.setLatencyP50Ms(latencyP50);
        lat.setLatencyP95Ms(latencyP95);
        lat.setLatencyP99Ms(latencyP99);
        lat.setLatencyP99P50Ratio(latencyP99P50Ratio);

        // GC
        GcFeatures gc = payload.getGcFeatures();
        gc.setGcEventCount(gcEventCount);
        gc.setGcPauseMaxMs(gcPauseMax);
        gc.setGcPauseAvgMs(gcPauseAvg);

        // Dependencies
        DependencyFeatures dep = payload.getDependencyFeatures();
        dep.setConnectionFailureCount(connectionFailures);
        dep.setConnectionWaitAvgMs(connectionWaitAvg);
        dep.setRetryTotal(retryTotal);

        // Distributions
        DistributionFeatures dist = payload.getDistributionFeatures();
        dist.setSeverityEntropy(severityEntropy);
        dist.setExceptionTypeCardinality(exceptionTypes.size());
        dist.setLogBodySizeAvgBytes(logBodySizeAvg);
        dist.setLogBodySizeP95Bytes(logBodySizeP95);

        // Category distribution
        payload.setCategoryDistribution(categoryDistribution);

        // Security
        payload.getSecurityFeatures().setSecurityEventCount(securityEvents);

        // Metadata
        FeatureMetadata meta = payload.getMetadata();
        meta.setAnomalyCount(anomalyCount);
        meta.setRepresentativeTraceIds(representativeTraceIds);
        meta.setCreatedAtEpochMs(System.currentTimeMillis());

        out.collect(payload);

        LOG.debug("ML feature vector emitted for service='{}' window=[{},{}] events={} error_rate={:.4f} velocity={:.6f}",
                serviceName, context.window().getStart(), context.window().getEnd(),
                totalEvents, errorRate, errorVelocity);
    }

    // ════════════════════════════════════════════════════════
    // Statistics Helpers
    // ════════════════════════════════════════════════════════

    /**
     * Shannon entropy: H = -Σ p_i * log2(p_i)
     * Measures the diversity/uniformity of the severity distribution.
     * High entropy = healthy (diverse severities). Low entropy = collapsing toward errors.
     */
    private static double computeEntropy(double[] counts) {
        double total = 0;
        for (double c : counts) total += c;
        if (total == 0) return 0.0;

        double entropy = 0.0;
        for (double c : counts) {
            if (c > 0) {
                double p = c / total;
                entropy -= p * (Math.log(p) / Math.log(2));
            }
        }
        return Math.round(entropy * 1000.0) / 1000.0; // 3 decimal places
    }

    /**
     * Compute the percentile from a pre-sorted list using nearest-rank method.
     */
    private static double percentile(List<Double> sortedValues, double pct) {
        if (sortedValues.isEmpty()) return 0.0;
        int index = (int) Math.ceil(pct * sortedValues.size()) - 1;
        index = Math.max(0, Math.min(index, sortedValues.size() - 1));
        return sortedValues.get(index);
    }

    /**
     * Compute the arithmetic mean.
     */
    private static double average(List<Double> values) {
        if (values.isEmpty()) return 0.0;
        double sum = 0;
        for (double v : values) sum += v;
        return sum / values.size();
    }
}
