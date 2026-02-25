package com.zrivot.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Map;

/**
 * Represents a single slice of an Elasticsearch query for parallel document fetching.
 * When a reflow query matches too many documents, it is split into multiple slices
 * that can be processed independently across Flink workers.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ReflowSlice implements Serializable {

    private static final long serialVersionUID = 1L;

    private String enricherName;
    private Map<String, Object> query;
    private int sliceId;
    private int maxSlices;
    private String index;

    /**
     * Creates a single (non-sliced) query — used when the document count is below threshold.
     */
    public static ReflowSlice unsliced(String enricherName, Map<String, Object> query, String index) {
        return new ReflowSlice(enricherName, query, 0, 1, index);
    }

    public boolean isSliced() {
        return maxSlices > 1;
    }
}
