package com.zrivot.config;

/**
 * Defines the pipeline execution mode.
 */
public enum PipelineMode {

    /**
     * Realtime pipeline: reads raw documents from Kafka AND runs reflow pipelines.
     */
    REALTIME,

    /**
     * Offline pipeline: only runs reflow pipelines (no Kafka raw document input).
     */
    OFFLINE
}
