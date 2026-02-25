package com.zrivot.pipeline;

import com.zrivot.config.EnricherConfig;
import com.zrivot.config.PipelineConfig;
import com.zrivot.config.PipelineMode;
import com.zrivot.joiner.EnrichmentJoinerFunction;
import com.zrivot.kafka.KafkaSinkFactory;
import com.zrivot.kafka.KafkaSourceFactory;
import com.zrivot.model.EnrichedDocument;
import com.zrivot.model.EnrichmentResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Orchestrates the full enrichment pipeline based on the configured {@link PipelineMode}.
 *
 * <p><strong>REALTIME mode:</strong>
 * <ul>
 *   <li>For each enricher: builds a realtime sub-pipeline (Kafka raw → enrich) AND
 *       a reflow sub-pipeline (Kafka reflow → ES fetch → enrich)</li>
 *   <li>Unions all enrichment result streams</li>
 *   <li>Joins results per documentId in the {@link EnrichmentJoinerFunction}</li>
 *   <li>Writes enriched documents to the Kafka output topic</li>
 * </ul>
 *
 * <p><strong>OFFLINE mode:</strong>
 * <ul>
 *   <li>For each enricher: builds only the reflow sub-pipeline</li>
 *   <li>No Kafka raw input is created</li>
 *   <li>Everything else is the same as realtime</li>
 * </ul>
 *
 * <p>Each enricher operates independently. If one enricher's Kafka consumer lags or its
 * enrichment API fails, it does NOT affect the others — the joiner will still emit
 * partial results after a configurable timeout.</p>
 */
@Slf4j
public class PipelineOrchestrator {

    private final PipelineConfig config;

    public PipelineOrchestrator(PipelineConfig config) {
        this.config = config;
    }

    /**
     * Builds and wires the complete pipeline topology in the given Flink environment.
     */
    public void build(StreamExecutionEnvironment env) {
        KafkaSourceFactory kafkaSourceFactory = new KafkaSourceFactory(config);
        KafkaSinkFactory kafkaSinkFactory = new KafkaSinkFactory(config);

        List<EnricherConfig> enrichers = config.getEnrichers();
        PipelineMode mode = config.getMode();

        log.info("Building pipeline in {} mode with {} enrichers", mode, enrichers.size());

        Set<String> enricherNames = enrichers.stream()
                .map(EnricherConfig::getName)
                .collect(Collectors.toSet());

        RealtimePipelineBuilder realtimeBuilder = new RealtimePipelineBuilder(env, config, kafkaSourceFactory);
        ReflowPipelineBuilder reflowBuilder = new ReflowPipelineBuilder(env, config, kafkaSourceFactory);

        List<DataStream<EnrichmentResult>> allResultStreams = new ArrayList<>();

        for (EnricherConfig enricherConfig : enrichers) {
            // Build reflow pipeline (always present in both modes)
            DataStream<EnrichmentResult> reflowResults = reflowBuilder.build(enricherConfig);
            allResultStreams.add(reflowResults);

            // Build realtime pipeline (only in REALTIME mode)
            if (mode == PipelineMode.REALTIME) {
                DataStream<EnrichmentResult> realtimeResults = realtimeBuilder.build(enricherConfig);
                allResultStreams.add(realtimeResults);
            }
        }

        // Union all enrichment result streams
        DataStream<EnrichmentResult> unifiedResults = unionStreams(allResultStreams);

        // Join all enrichments per document and emit
        DataStream<EnrichedDocument> enrichedDocs = unifiedResults
                .keyBy(EnrichmentResult::getDocumentId)
                .process(new EnrichmentJoinerFunction(enricherNames, config.getJoinerTimeoutMs()))
                .name("enrichment-joiner");

        // Write to Kafka output
        enrichedDocs.sinkTo(kafkaSinkFactory.createSink())
                .name("kafka-output-sink");

        log.info("Pipeline topology built successfully");
    }

    /**
     * Unions multiple data streams into one.
     */
    private DataStream<EnrichmentResult> unionStreams(List<DataStream<EnrichmentResult>> streams) {
        if (streams.isEmpty()) {
            throw new IllegalStateException("No enrichment streams to union");
        }
        if (streams.size() == 1) {
            return streams.get(0);
        }

        DataStream<EnrichmentResult> result = streams.get(0);
        for (int i = 1; i < streams.size(); i++) {
            result = result.union(streams.get(i));
        }
        return result;
    }
}
