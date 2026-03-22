package com.idop.flink.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.idop.flink.models.TelemetryEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Deserializes JSON telemetry events from the Kafka/Redpanda topic.
 */
public class TelemetryDeserializer implements DeserializationSchema<TelemetryEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(TelemetryDeserializer.class);
    private static final long serialVersionUID = 1L;

    private transient ObjectMapper mapper;

    private ObjectMapper getMapper() {
        if (mapper == null) {
            mapper = new ObjectMapper();
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        }
        return mapper;
    }

    @Override
    public TelemetryEvent deserialize(byte[] message) throws IOException {
        try {
            return getMapper().readValue(message, TelemetryEvent.class);
        } catch (Exception e) {
            LOG.warn("Failed to deserialize telemetry event: {}", e.getMessage());
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(TelemetryEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<TelemetryEvent> getProducedType() {
        return TypeInformation.of(TelemetryEvent.class);
    }
}
