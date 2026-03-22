package com.idop.flink.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.idop.flink.models.MLFeaturePayload;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Serializes MLFeaturePayload to JSON bytes for the telemetry-ml-features topic.
 */
public class MLFeatureSerializer implements SerializationSchema<MLFeaturePayload> {

    private static final Logger LOG = LoggerFactory.getLogger(MLFeatureSerializer.class);
    private static final long serialVersionUID = 1L;

    private transient ObjectMapper mapper;

    private ObjectMapper getMapper() {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        return mapper;
    }

    @Override
    public byte[] serialize(MLFeaturePayload element) {
        try {
            return getMapper().writeValueAsBytes(element);
        } catch (Exception e) {
            LOG.error("Failed to serialize MLFeaturePayload: {}", e.getMessage());
            return new byte[0];
        }
    }
}
