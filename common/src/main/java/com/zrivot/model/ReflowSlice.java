package com.zrivot.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;

/**
 * Represents a single slice of an Elasticsearch query for parallel document fetching.
 * When a reflow query matches too many documents, it is split into multiple slices
 * that can be processed independently across Flink workers.
 *
 * <p>Each group of slices produced from a single reflow message shares the same
 * {@code requestId} (a UUID), ensuring that slices from different reflow messages
 * never collide when used as Flink keys.</p>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ReflowSlice implements Serializable {

    private static final long serialVersionUID = 2L;

    /** Unique identifier for the reflow request that produced this slice. */
    private String requestId;

    private String enricherName;
    private Map<String, Object> query;
    private int sliceId;
    private int maxSlices;
    private String index;

    /**
     * Creates a single (non-sliced) query — used when the document count is below threshold.
     */
    public static ReflowSlice unsliced(String requestId, String enricherName,
                                       Map<String, Object> query, String index) {
        return new ReflowSlice(requestId, enricherName, query, 0, 1, index);
    }

    public boolean isSliced() {
        return maxSlices > 1;
    }

    /**
     * Returns a composite key that uniquely identifies this slice across all reflow requests.
     * Used as the Flink keyBy selector for stateful document fetching.
     */
    public String getSliceKey() {
        return requestId + ":" + sliceId;
    }
}
