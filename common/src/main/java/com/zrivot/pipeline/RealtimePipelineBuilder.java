package com.zrivot.pipeline;

import com.zrivot.config.EnricherConfig;
import com.zrivot.config.PipelineConfig;
import com.zrivot.enrichment.AsyncBulkEnrichmentFunction;
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
 *       → keyBy(documentId) → BoomerangEnrichmentFunction  (only when reflowEnabled)
 *       → AsyncDataStream.orderedWait
 *             single-doc : {@link AsyncEnrichmentFunction}
 *             bulk       : {@link AsyncBulkEnrichmentFunction}
 * </pre>
 *
 * <h3>Key design decisions</h3>
 * <ul>
 *   <li><b>orderedWait</b> — emits results in the same order as input, keeping
 *       downstream semantics deterministic.</li>
 *   <li><b>No {@code keyBy("bulk")}</b> — the stream is <em>not</em> funnelled
 *       into a single key; Flink naturally partitions work across task slots.</li>
 *   <li><b>Rate limiting + concurrency control</b> — handled inside the async
 *       functions via {@link com.zrivot.enrichment.EnrichmentRateLimiter} and
 *       {@link com.zrivot.enrichment.SharedEnrichmentExecutor}.</li>
 * </ul>
 */
@Slf4j
public class RealtimePipelineBuilder {

    private final StreamExecutionEnvironment env;
    private final PipelineConfig config;
    private final KafkaSourceFactory kafkaSourceFactory;

    public RealtimePipelineBuilder(StreamExecutionEnvironment env,
                                   PipelineConfig config,
                                   KafkaSourceFactory kafkaSourceFactory) {
        this.env = env;
        this.config = config;
        this.kafkaSourceFactory = kafkaSourceFactory;
    }

    /**
     * Builds the realtime enrichment pipeline for one enricher and returns
     * the enrichment result stream.
     */
    public DataStream<EnrichmentResult> build(EnricherConfig enricherConfig) {
        String enricherName = enricherConfig.getName();
        log.info("Building realtime pipeline for enricher '{}'", enricherName);

        var rawSource = kafkaSourceFactory.createRawSource(
                config.getRawTopic(),
                enricherConfig.getConsumerGroup()
        );

        DataStream<RawDocument> rawDocs = env
                .fromSource(rawSource, WatermarkStrategy.noWatermarks(),
                        "realtime-source-" + enricherName);

        // Boomerang guard only needed when reflow is active
        DataStream<RawDocument> enricherInput;
        if (enricherConfig.isReflowEnabled()) {
            enricherInput = rawDocs
                    .keyBy(RawDocument::getDocumentId)
                    .process(new BoomerangEnrichmentFunction(enricherConfig))
                    .name("realtime-guard-" + enricherName);
        } else {
            enricherInput = rawDocs;
        }

        int maxConcurrent = config.getMaxConcurrentApiRequests();

        // Both bulk and single paths use AsyncDataStream.orderedWait — no keyBy("bulk")
        if (enricherConfig.isBulkEnabled()) {
            return AsyncDataStream.orderedWait(
                    enricherInput,
                    new AsyncBulkEnrichmentFunction(
                            enricherConfig,
                            config.getElasticsearch(),
                            config.getElasticsearch().getIndex(),
                            maxConcurrent),
                    enricherConfig.getAsyncTimeoutMs(),
                    TimeUnit.MILLISECONDS,
                    enricherConfig.getAsyncCapacity()
            ).name("realtime-bulk-enrich-" + enricherName)
             .setParallelism(enricherConfig.getParallelism());
        } else {
            return AsyncDataStream.orderedWait(
                    enricherInput,
                    new AsyncEnrichmentFunction(
                            enricherConfig,
                            config.getElasticsearch(),
                            config.getElasticsearch().getIndex(),
                            maxConcurrent),
                    enricherConfig.getAsyncTimeoutMs(),
                    TimeUnit.MILLISECONDS,
                    enricherConfig.getAsyncCapacity()
            ).name("realtime-async-enrich-" + enricherName)
             .setParallelism(enricherConfig.getParallelism());
        }
    }
}
