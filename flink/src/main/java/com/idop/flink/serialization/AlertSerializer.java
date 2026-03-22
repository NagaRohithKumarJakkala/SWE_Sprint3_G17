package com.idop.flink.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.idop.flink.models.AlertEvent;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Serializes AlertEvent to JSON bytes for the alerts-critical topic.
 */
public class AlertSerializer implements SerializationSchema<AlertEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(AlertSerializer.class);
    private static final long serialVersionUID = 1L;

    private transient ObjectMapper mapper;

    private ObjectMapper getMapper() {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        return mapper;
    }

    @Override
    public byte[] serialize(AlertEvent element) {
        try {
            return getMapper().writeValueAsBytes(element);
        } catch (Exception e) {
            LOG.error("Failed to serialize AlertEvent: {}", e.getMessage());
            return new byte[0];
        }
    }
}
