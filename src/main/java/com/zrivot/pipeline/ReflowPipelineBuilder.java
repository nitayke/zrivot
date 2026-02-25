package com.zrivot.pipeline;

import com.zrivot.config.EnricherConfig;
import com.zrivot.config.PipelineConfig;
import com.zrivot.enrichment.BoomerangEnrichmentFunction;
import com.zrivot.kafka.KafkaSourceFactory;
import com.zrivot.model.EnrichmentResult;
import com.zrivot.model.RawDocument;
import com.zrivot.model.ReflowSlice;
import com.zrivot.reflow.ReflowCountAndSliceFunction;
import com.zrivot.reflow.ReflowDocumentFetchFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Builds the reflow sub-pipeline for a single enricher.
 *
 * <p>Reflow pipeline per enricher:
 * <pre>
 *   [Reflow Kafka Topic]
 *       → ReflowCountAndSlice (count docs, decide whether to slice)
 *       → keyBy(sliceId) for parallel ES fetch
 *       → ReflowDocumentFetch (retrieve docs from ES)
 *       → keyBy(documentId) for boomerang-guarded enrichment
 *       → BoomerangEnrichmentFunction
 * </pre>
 */
@Slf4j
public class ReflowPipelineBuilder {

    private final StreamExecutionEnvironment env;
    private final PipelineConfig config;
    private final KafkaSourceFactory kafkaSourceFactory;

    public ReflowPipelineBuilder(StreamExecutionEnvironment env,
                                 PipelineConfig config,
                                 KafkaSourceFactory kafkaSourceFactory) {
        this.env = env;
        this.config = config;
        this.kafkaSourceFactory = kafkaSourceFactory;
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

        // 3. KeyBy sliceId for parallel document fetching
        DataStream<RawDocument> reflowDocs = slices
                .keyBy(ReflowSlice::getSliceId)
                .process(new ReflowDocumentFetchFunction(
                        config.getElasticsearch(),
                        config.getReflow()
                ))
                .name("reflow-fetch-" + enricherName);

        // 4. KeyBy documentId and run through boomerang-guarded enrichment
        return reflowDocs
                .keyBy(RawDocument::getDocumentId)
                .process(new BoomerangEnrichmentFunction(enricherConfig))
                .name("reflow-enrich-" + enricherName);
    }
}
