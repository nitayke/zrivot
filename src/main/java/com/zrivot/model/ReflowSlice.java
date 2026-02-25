package com.zrivot.model;

import java.io.Serializable;
import java.util.Map;

/**
 * Represents a single slice of an Elasticsearch query for parallel document fetching.
 * When a reflow query matches too many documents, it is split into multiple slices
 * that can be processed independently across Flink workers.
 */
public class ReflowSlice implements Serializable {

    private static final long serialVersionUID = 1L;

    private String enricherName;
    private Map<String, Object> query;
    private int sliceId;
    private int maxSlices;
    private String index;

    public ReflowSlice() {
    }

    public ReflowSlice(String enricherName, Map<String, Object> query,
                       int sliceId, int maxSlices, String index) {
        this.enricherName = enricherName;
        this.query = query;
        this.sliceId = sliceId;
        this.maxSlices = maxSlices;
        this.index = index;
    }

    /**
     * Creates a single (non-sliced) query — used when the document count is below threshold.
     */
    public static ReflowSlice unsliced(String enricherName, Map<String, Object> query, String index) {
        return new ReflowSlice(enricherName, query, 0, 1, index);
    }

    public String getEnricherName() {
        return enricherName;
    }

    public void setEnricherName(String enricherName) {
        this.enricherName = enricherName;
    }

    public Map<String, Object> getQuery() {
        return query;
    }

    public void setQuery(Map<String, Object> query) {
        this.query = query;
    }

    public int getSliceId() {
        return sliceId;
    }

    public void setSliceId(int sliceId) {
        this.sliceId = sliceId;
    }

    public int getMaxSlices() {
        return maxSlices;
    }

    public void setMaxSlices(int maxSlices) {
        this.maxSlices = maxSlices;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public boolean isSliced() {
        return maxSlices > 1;
    }

    @Override
    public String toString() {
        return "ReflowSlice{enricherName='" + enricherName +
                "', sliceId=" + sliceId + "/" + maxSlices + ", index='" + index + "'}";
    }
}
