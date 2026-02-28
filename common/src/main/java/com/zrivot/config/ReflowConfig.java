package com.zrivot.config;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Configuration for the reflow (offline re-enrichment) subsystem.
 */
@Data
@NoArgsConstructor
public class ReflowConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    /** If the ES query matches more than this many documents, slice the query. */
    private int sliceThreshold = 10_000;

    /** Maximum number of slices when splitting a large query. */
    private int maxSlices = 10;

    /** Batch size for fetching documents from Elasticsearch. */
    private int fetchBatchSize = 1000;

    /**
     * The document field name used for primary sort in search_after pagination.
     * Documents are sorted by this field descending (newest first), then by {@code _id}.
     * Set to {@code null} or empty to sort only by {@code _id}.
     */
    private String sortField;
}
