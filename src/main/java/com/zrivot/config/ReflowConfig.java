package com.zrivot.config;

import java.io.Serializable;

/**
 * Configuration for the reflow (offline re-enrichment) subsystem.
 */
public class ReflowConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    /** If the ES query matches more than this many documents, slice the query. */
    private int sliceThreshold = 10_000;

    /** Maximum number of slices when splitting a large query. */
    private int maxSlices = 10;

    /** Batch size for fetching documents from Elasticsearch. */
    private int fetchBatchSize = 1000;

    public ReflowConfig() {
    }

    public int getSliceThreshold() {
        return sliceThreshold;
    }

    public void setSliceThreshold(int sliceThreshold) {
        this.sliceThreshold = sliceThreshold;
    }

    public int getMaxSlices() {
        return maxSlices;
    }

    public void setMaxSlices(int maxSlices) {
        this.maxSlices = maxSlices;
    }

    public int getFetchBatchSize() {
        return fetchBatchSize;
    }

    public void setFetchBatchSize(int fetchBatchSize) {
        this.fetchBatchSize = fetchBatchSize;
    }
}
