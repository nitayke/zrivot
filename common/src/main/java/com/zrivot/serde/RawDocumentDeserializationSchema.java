package com.zrivot.serde;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zrivot.model.RawDocument;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * Deserializes raw Kafka message bytes into {@code RawDocument<T>} where {@code T}
 * is the domain document class (e.g. {@code PurchaseDocument}).
 *
 * <p>Uses Jackson's parametric type support to properly deserialize the generic
 * {@code payload} field into the configured type.</p>
 *
 * @param <T> the domain document type
 */
public class RawDocumentDeserializationSchema<T> implements DeserializationSchema<RawDocument<T>> {

    private static final long serialVersionUID = 1L;

    private final Class<T> documentClass;
    private transient ObjectMapper objectMapper;
    private transient JavaType javaType;

    public RawDocumentDeserializationSchema(Class<T> documentClass) {
        this.documentClass = documentClass;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        initMapper();
    }

    @Override
    public RawDocument<T> deserialize(byte[] message) throws IOException {
        if (objectMapper == null) {
            initMapper();
        }
        return objectMapper.readValue(message, javaType);
    }

    @Override
    public boolean isEndOfStream(RawDocument<T> nextElement) {
        return false;
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypeInformation<RawDocument<T>> getProducedType() {
        return (TypeInformation<RawDocument<T>>) (TypeInformation<?>) TypeInformation.of(RawDocument.class);
    }

    private void initMapper() {
        objectMapper = new ObjectMapper();
        javaType = objectMapper.getTypeFactory()
                .constructParametricType(RawDocument.class, documentClass);
    }
}
