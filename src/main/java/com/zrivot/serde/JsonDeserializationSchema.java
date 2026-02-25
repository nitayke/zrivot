package com.zrivot.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * Generic JSON deserialisation schema backed by Jackson.
 * Converts raw Kafka message bytes into a typed Java object.
 *
 * @param <T> target type
 */
public class JsonDeserializationSchema<T> implements DeserializationSchema<T> {

    private static final long serialVersionUID = 1L;

    private final Class<T> targetClass;
    private transient ObjectMapper objectMapper;

    public JsonDeserializationSchema(Class<T> targetClass) {
        this.targetClass = targetClass;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        objectMapper = new ObjectMapper();
    }

    @Override
    public T deserialize(byte[] message) throws IOException {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        return objectMapper.readValue(message, targetClass);
    }

    @Override
    public boolean isEndOfStream(T nextElement) {
        return false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(targetClass);
    }
}
