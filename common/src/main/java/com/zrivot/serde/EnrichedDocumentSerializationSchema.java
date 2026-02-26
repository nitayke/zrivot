package com.zrivot.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zrivot.model.EnrichedDocument;
import org.apache.flink.api.common.serialization.SerializationSchema;

/**
 * Serialises an {@link EnrichedDocument} to JSON bytes for the Kafka output sink.
 * Produces the merged representation (original payload + enrichments).
 */
public class EnrichedDocumentSerializationSchema implements SerializationSchema<EnrichedDocument> {

    private static final long serialVersionUID = 1L;

    private transient ObjectMapper objectMapper;

    @Override
    public void open(InitializationContext context) throws Exception {
        objectMapper = new ObjectMapper();
    }

    @Override
    public byte[] serialize(EnrichedDocument document) {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        try {
            return objectMapper.writeValueAsBytes(document.toMergedMap());
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialise EnrichedDocument " +
                    document.getDocumentId(), e);
        }
    }
}
