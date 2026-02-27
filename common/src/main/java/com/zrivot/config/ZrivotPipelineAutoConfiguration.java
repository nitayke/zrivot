package com.zrivot.config;

import com.zrivot.kafka.KafkaSinkFactory;
import com.zrivot.kafka.KafkaSourceFactory;
import com.zrivot.pipeline.PipelineOrchestrator;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring configuration that wires the pipeline beans.
 *
 * <p>Discovered via component-scanning from each domain {@code @SpringBootApplication}
 * (which scans {@code com.zrivot.*}).  Beans defined here are <strong>driver-side</strong>
 * only – they set up the Flink topology before job submission.  Flink operators that run
 * on task managers remain plain serialisable objects.</p>
 */
@Configuration
@EnableConfigurationProperties(PipelineConfig.class)
public class ZrivotPipelineAutoConfiguration {

    @Bean
    public KafkaSourceFactory kafkaSourceFactory(PipelineConfig config) {
        return new KafkaSourceFactory(config);
    }

    @Bean
    public KafkaSinkFactory kafkaSinkFactory(PipelineConfig config) {
        return new KafkaSinkFactory(config);
    }

    @Bean
    public PipelineOrchestrator pipelineOrchestrator(PipelineConfig config,
                                                     KafkaSourceFactory kafkaSourceFactory,
                                                     KafkaSinkFactory kafkaSinkFactory) {
        return new PipelineOrchestrator(config, kafkaSourceFactory, kafkaSinkFactory);
    }
}
