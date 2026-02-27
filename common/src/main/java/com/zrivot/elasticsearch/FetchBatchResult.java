package com.zrivot.elasticsearch;

import com.zrivot.model.RawDocument;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

/**
 * Holds the result of a single Elasticsearch search_after batch.
 *
 * <p>{@code nextSearchAfter} is {@code null} when there are no more pages to fetch,
 * signalling that the caller should stop paginating.</p>
 *
 * @param <T> the domain document type
 */
@Data
@AllArgsConstructor
public class FetchBatchResult<T> {

    /** Documents returned in this batch. */
    private final List<RawDocument<T>> documents;

    /**
     * Sort values of the last hit — used as the {@code search_after} cursor for the next batch.
     * {@code null} when this was the final batch (no more results).
     */
    private final List<String> nextSearchAfter;

    /** @return {@code true} if more batches remain to be fetched. */
    public boolean hasMore() {
        return nextSearchAfter != null;
    }
}
