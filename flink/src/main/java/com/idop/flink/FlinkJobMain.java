package com.idop.flink;

import com.idop.flink.jobs.HotPathJob;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for the IDOP Flink Ingestion Hot Path.
 *
 * Launches the hot-path streaming job that consumes from telemetry-hot,
 * computes windowed aggregations, enriches with ClickHouse context,
 * and publishes to telemetry-ml-features and alerts-critical topics.
 */
public class FlinkJobMain {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkJobMain.class);

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        String jobName = params.get("job", "hot-path");

        LOG.info("Starting IDOP Flink job: {}", jobName);

        switch (jobName) {
            case "hot-path":
                HotPathJob.run(params);
                break;
            default:
                LOG.error("Unknown job name: {}. Supported: hot-path", jobName);
                System.exit(1);
        }
    }
}
