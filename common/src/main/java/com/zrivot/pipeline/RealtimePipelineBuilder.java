package com.zrivot.pipeline;

import com.zrivot.config.EnricherConfig;
import com.zrivot.config.PipelineConfig;
import com.zrivot.enrichment.AsyncEnrichmentFunction;
import com.zrivot.enrichment.BoomerangEnrichmentFunction;
import com.zrivot.kafka.KafkaSourceFactory;
import com.zrivot.model.EnrichmentResult;
import com.zrivot.model.RawDocument;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * Builds the realtime enrichment sub-pipeline for a single enricher.
 *
 * <p>Realtime pipeline per enricher:
 * <pre>
 *   [Raw Kafka Topic]
 *       → keyBy(documentId)
 *       → BoomerangEnrichmentFunction  (keyed state guard — only when reflowEnabled)
 *       → AsyncDataStream              (non-blocking API enrichment)
 * </pre>
 *
 * <p>Each enricher reads from the same raw topic but uses its own consumer group,
 * so they can lag independently without affecting each other.</p>
 */
@Slf4j
public class RealtimePipelineBuilder {

    private final StreamExecutionEnvironment env;
    private final PipelineConfig config;
    private final KafkaSourceFactory kafkaSourceFactory;
    private final Class<?> documentClass;

    public RealtimePipelineBuilder(StreamExecutionEnvironment env,
                                   PipelineConfig config,
                                   KafkaSourceFactory kafkaSourceFactory,
                                   Class<?> documentClass) {
        this.env = env;
        this.config = config;
        this.kafkaSourceFactory = kafkaSourceFactory;
        this.documentClass = documentClass;
    }

    /**
     * Builds the realtime enrichment pipeline for one enricher and returns
     * the enrichment result stream.
     */
    public DataStream<EnrichmentResult> build(EnricherConfig enricherConfig) {
        String enricherName = enricherConfig.getName();
        log.info("Building realtime pipeline for enricher '{}'", enricherName);

        // Read raw documents from Kafka with the enricher's own consumer group
        var rawSource = kafkaSourceFactory.createRawSource(
                config.getRawTopic(),
                enricherConfig.getConsumerGroup(),
                documentClass
        );

        DataStream<RawDocument<?>> rawDocs = env
                .fromSource(rawSource, WatermarkStrategy.noWatermarks(),
                        "realtime-source-" + enricherName);

        // Determine input stream for async enrichment:
        // - With reflow: apply boomerang guard (keyed state filters stale events)
        // - Without reflow: no guard needed — pass raw docs straight through
        DataStream<RawDocument<?>> enricherInput;
        if (enricherConfig.isReflowEnabled()) {
            enricherInput = rawDocs
                    .keyBy(RawDocument::getDocumentId)
                    .process(new BoomerangEnrichmentFunction(enricherConfig))
                    .name("realtime-guard-" + enricherName);
        } else {
            enricherInput = rawDocs;
        }

        // Async enrichment — non-blocking API calls via AsyncDataStream
        return AsyncDataStream.unorderedWait(
                enricherInput,
                new AsyncEnrichmentFunction(enricherConfig),
                enricherConfig.getAsyncTimeoutMs(),
                TimeUnit.MILLISECONDS,
                enricherConfig.getAsyncCapacity()
        ).name("realtime-async-enrich-" + enricherName);
    }
}
