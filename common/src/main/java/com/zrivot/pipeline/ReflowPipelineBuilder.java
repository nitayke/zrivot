package com.zrivot.pipeline;

import com.zrivot.config.EnricherConfig;
import com.zrivot.config.PipelineConfig;
import com.zrivot.enrichment.AsyncEnrichmentFunction;
import com.zrivot.enrichment.BoomerangEnrichmentFunction;
import com.zrivot.kafka.KafkaSourceFactory;
import com.zrivot.model.EnrichmentResult;
import com.zrivot.model.RawDocument;
import com.zrivot.model.ReflowSlice;
import com.zrivot.reflow.ReflowCountAndSliceFunction;
import com.zrivot.reflow.ReflowDocumentFetchFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * Builds the reflow sub-pipeline for a single enricher.
 *
 * <p>Reflow pipeline per enricher:
 * <pre>
 *   [Reflow Kafka Topic]
 *       → ReflowCountAndSlice (count docs, decide whether to slice)
 *       → keyBy(sliceKey) for parallel stateful ES fetch
 *       → ReflowDocumentFetch (checkpoint-recoverable search_after)
 *       → keyBy(documentId) for boomerang guard
 *       → BoomerangEnrichmentFunction (keyed state guard)
 *       → AsyncDataStream (non-blocking enrichment)
 * </pre>
 */
@Slf4j
public class ReflowPipelineBuilder {

    private final StreamExecutionEnvironment env;
    private final PipelineConfig config;
    private final KafkaSourceFactory kafkaSourceFactory;
    private final Class<?> documentClass;

    public ReflowPipelineBuilder(StreamExecutionEnvironment env,
                                 PipelineConfig config,
                                 KafkaSourceFactory kafkaSourceFactory,
                                 Class<?> documentClass) {
        this.env = env;
        this.config = config;
        this.kafkaSourceFactory = kafkaSourceFactory;
        this.documentClass = documentClass;
    }

    /**
     * Builds the reflow pipeline for one enricher and returns the enrichment result stream.
     */
    public DataStream<EnrichmentResult> build(EnricherConfig enricherConfig) {
        String enricherName = enricherConfig.getName();
        log.info("Building reflow pipeline for enricher '{}'", enricherName);

        // 1. Read reflow messages from the enricher's dedicated reflow Kafka topic
        var reflowSource = kafkaSourceFactory.createReflowSource(
                enricherConfig.getReflowTopic(),
                enricherConfig.getReflowConsumerGroup()
        );

        var reflowMessages = env
                .fromSource(reflowSource, WatermarkStrategy.noWatermarks(),
                        "reflow-source-" + enricherName);

        // 2. Count documents and split into slices if needed
        DataStream<ReflowSlice> slices = reflowMessages
                .process(new ReflowCountAndSliceFunction(
                        config.getElasticsearch(),
                        config.getReflow(),
                        config.getElasticsearch().getIndex()
                ))
                .name("reflow-count-slice-" + enricherName);

        // 3. KeyBy unique sliceKey for parallel, stateful document fetching
        DataStream<RawDocument<?>> reflowDocs = slices
                .keyBy(ReflowSlice::getSliceKey)
                .process(new ReflowDocumentFetchFunction<>(
                        config.getElasticsearch(),
                        config.getReflow(),
                        documentClass
                ))
                .name("reflow-fetch-" + enricherName);

        // 4. KeyBy documentId → boomerang guard (uses keyed state to filter stale events)
        DataStream<RawDocument<?>> guardedDocs = reflowDocs
                .keyBy(RawDocument::getDocumentId)
                .process(new BoomerangEnrichmentFunction(enricherConfig))
                .name("reflow-guard-" + enricherName);

        // 5. Async enrichment — non-blocking API calls via AsyncDataStream
        return AsyncDataStream.unorderedWait(
                guardedDocs,
                new AsyncEnrichmentFunction(enricherConfig),
                enricherConfig.getAsyncTimeoutMs(),
                TimeUnit.MILLISECONDS,
                enricherConfig.getAsyncCapacity()
        ).name("reflow-async-enrich-" + enricherName);
    }
}
