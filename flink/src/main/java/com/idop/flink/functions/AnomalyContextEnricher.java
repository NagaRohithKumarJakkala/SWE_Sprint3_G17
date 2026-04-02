package com.idop.flink.functions;

import com.idop.flink.models.MLFeaturePayload;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

/**
 * REQ-2.7: Upon detecting anomalies in the hot stream, executes a
 * dynamic query against ClickHouse to fetch historical log context
 * for the specific time window and service.
 *
 * REQ-2.8: Bundles the anomalous stream events with the fetched
 * ClickHouse context and publishes the enriched payload.
 */
public class AnomalyContextEnricher
        extends KeyedProcessFunction<String, MLFeaturePayload, MLFeaturePayload> {

    private static final Logger LOG = LoggerFactory.getLogger(AnomalyContextEnricher.class);
    private static final long serialVersionUID = 1L;

    private final String clickhouseUrl;
    private transient Connection connection;

    // Context lookup query: fetch recent logs for the service in the anomaly window
    private static final String CONTEXT_QUERY =
            "SELECT timestamp, trace_id, severity, body, ml_features " +
            "FROM telemetry.logs " +
            "WHERE service_name = ? " +
            "  AND timestamp >= toDateTime64(?, 9, 'UTC') " +
            "  AND timestamp <= toDateTime64(?, 9, 'UTC') " +
            "ORDER BY timestamp DESC " +
            "LIMIT 100";

    public AnomalyContextEnricher(String clickhouseUrl) {
        this.clickhouseUrl = clickhouseUrl;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        try {
            connection = DriverManager.getConnection(clickhouseUrl);
            LOG.info("Connected to ClickHouse at {}", clickhouseUrl);
        } catch (Exception e) {
            LOG.warn("Could not connect to ClickHouse. Context enrichment will be skipped: {}", e.getMessage());
        }
    }

    @Override
    public void processElement(MLFeaturePayload payload,
                               KeyedProcessFunction<String, MLFeaturePayload, MLFeaturePayload>.Context ctx,
                               Collector<MLFeaturePayload> out) throws Exception {

        // Only enrich if there are anomalies and connection is available
        if (payload.getMetadata().getAnomalyCount() > 0 && connection != null && !connection.isClosed()) {
            try {
                List<Map<String, Object>> context = fetchContext(
                        payload.getServiceName(),
                        payload.getWindowStartEpochMs(),
                        payload.getWindowEndEpochMs()
                );
                payload.setHistoricalContext(context);
            } catch (Exception e) {
                LOG.warn("Failed to fetch ClickHouse context for service '{}': {}",
                        payload.getServiceName(), e.getMessage());
                payload.setHistoricalContext(Collections.emptyList());
            }
        } else {
            payload.setHistoricalContext(Collections.emptyList());
        }

        out.collect(payload);
    }

    private List<Map<String, Object>> fetchContext(String serviceName, long windowStart, long windowEnd)
            throws Exception {
        List<Map<String, Object>> results = new ArrayList<>();

        // Convert epoch millis to seconds for ClickHouse DateTime
        double startSec = windowStart / 1000.0;
        double endSec = windowEnd / 1000.0;

        try (PreparedStatement stmt = connection.prepareStatement(CONTEXT_QUERY)) {
            stmt.setString(1, serviceName);
            stmt.setDouble(2, startSec);
            stmt.setDouble(3, endSec);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    row.put("timestamp", rs.getString("timestamp"));
                    row.put("trace_id", rs.getString("trace_id"));
                    row.put("severity", rs.getString("severity"));
                    row.put("body", rs.getString("body"));
                    row.put("ml_features", rs.getString("ml_features"));
                    results.add(row);
                }
            }
        }

        return results;
    }

    @Override
    public void close() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
        super.close();
    }
}
