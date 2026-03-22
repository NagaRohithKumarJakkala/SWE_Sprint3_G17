package com.idop.flink.functions;

import com.idop.flink.models.TelemetryEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;

import java.time.Instant;

/**
 * Extracts event-time timestamps from TelemetryEvent.
 * REQ-2.10: Event-time watermarking support.
 */
public class TelemetryTimestampExtractor implements SerializableTimestampAssigner<TelemetryEvent> {

    private static final long serialVersionUID = 1L;

    @Override
    public long extractTimestamp(TelemetryEvent event, long recordTimestamp) {
        try {
            if (event.getTimestamp() != null) {
                // Try parsing as ISO-8601 or epoch millis
                String ts = event.getTimestamp();
                if (ts.contains("T") || ts.contains("-")) {
                    return Instant.parse(ts).toEpochMilli();
                } else {
                    long val = Long.parseLong(ts);
                    // If nanoseconds, convert to millis
                    if (val > 1_000_000_000_000_000L) {
                        return val / 1_000_000;
                    }
                    return val;
                }
            }
        } catch (Exception e) {
            // Fall back to record timestamp
        }
        return recordTimestamp;
    }
}
