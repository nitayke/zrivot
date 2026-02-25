package com.zrivot.kafka;

import com.zrivot.config.PipelineConfig;
import com.zrivot.model.EnrichedDocument;
import com.zrivot.serde.EnrichedDocumentSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

import java.io.Serializable;

/**
 * Factory that builds the Kafka output sink for enriched documents.
 */
public class KafkaSinkFactory implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String bootstrapServers;
    private final String outputTopic;

    public KafkaSinkFactory(PipelineConfig config) {
        this.bootstrapServers = config.getBootstrapServers();
        this.outputTopic = config.getOutputTopic();
    }

    /**
     * Creates the Kafka sink that writes fully-enriched documents to the output topic.
     */
    public KafkaSink<EnrichedDocument> createSink() {
        return KafkaSink.<EnrichedDocument>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(outputTopic)
                                .setValueSerializationSchema(new EnrichedDocumentSerializationSchema())
                                .build()
                )
                .build();
    }
}
