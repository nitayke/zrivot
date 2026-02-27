package com.zrivot.kafka;

import com.zrivot.config.PipelineConfig;
import com.zrivot.model.RawDocument;
import com.zrivot.model.ReflowMessage;
import com.zrivot.serde.JsonDeserializationSchema;
import com.zrivot.serde.RawDocumentDeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.Serializable;

/**
 * Factory that creates typed {@link KafkaSource} instances from pipeline configuration.
 * Centralises Kafka consumer setup so that enricher pipelines stay clean.
 */
public class KafkaSourceFactory implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String bootstrapServers;

    public KafkaSourceFactory(PipelineConfig config) {
        this.bootstrapServers = config.getBootstrapServers();
    }

    /**
     * Creates a Kafka source for raw documents with the given consumer group.
     *
     * @param topic         the Kafka topic to consume from
     * @param consumerGroup the consumer group id
     * @param documentClass the domain document class for typed payload deserialization
     * @param <T>           the domain document type
     */
    @SuppressWarnings("unchecked")
    public <T> KafkaSource<RawDocument<T>> createRawSource(String topic, String consumerGroup,
                                                           Class<T> documentClass) {
        return (KafkaSource<RawDocument<T>>) (KafkaSource<?>) KafkaSource.builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(consumerGroup)
                .setStartingOffsets(OffsetsInitializer.committedOffsets())
                .setValueOnlyDeserializer(new RawDocumentDeserializationSchema<>(documentClass))
                .build();
    }

    /**
     * Creates a Kafka source for reflow messages with the given consumer group.
     */
    public KafkaSource<ReflowMessage> createReflowSource(String topic, String consumerGroup) {
        return KafkaSource.<ReflowMessage>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(consumerGroup)
                .setStartingOffsets(OffsetsInitializer.committedOffsets())
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(ReflowMessage.class))
                .build();
    }
}
