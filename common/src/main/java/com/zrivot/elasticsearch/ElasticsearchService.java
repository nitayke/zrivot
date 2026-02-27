package com.zrivot.elasticsearch;

import com.zrivot.config.ElasticsearchConfig;
import com.zrivot.model.RawDocument;
import com.zrivot.model.ReflowSlice;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.SlicedScroll;
import co.elastic.clients.elasticsearch.core.CountRequest;
import co.elastic.clients.elasticsearch.core.CountResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Service that wraps Elasticsearch interactions for the reflow subsystem.
 *
 * <p>Responsible for:
 * <ul>
 *   <li>Counting documents matching a query (to decide whether to slice)</li>
 *   <li>Fetching documents in batches, with optional slice support</li>
 * </ul>
 *
 * <p>Instances are created per-operator and are NOT serialisable across Flink checkpoints;
 * they should be initialised inside {@code open()} methods.
 */
@Slf4j
public class ElasticsearchService implements Closeable {

    private final ElasticsearchClient client;
    private final RestClient restClient;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public ElasticsearchService(ElasticsearchConfig config) {
        HttpHost[] hosts = config.getHosts().stream()
                .map(HttpHost::create)
                .toArray(HttpHost[]::new);

        RestClientBuilder builder = RestClient.builder(hosts)
                .setRequestConfigCallback(rcb -> rcb
                        .setConnectTimeout(config.getConnectTimeoutMs())
                        .setSocketTimeout(config.getSocketTimeoutMs()));

        if (config.getUsername() != null && !config.getUsername().isEmpty()) {
            BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(config.getUsername(), config.getPassword()));
            builder.setHttpClientConfigCallback(hcb ->
                    hcb.setDefaultCredentialsProvider(credentialsProvider));
        }

        this.restClient = builder.build();
        RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        this.client = new ElasticsearchClient(transport);
    }

    /**
     * Counts how many documents in the given index match the provided query criteria.
     */
    public long countDocuments(String index, Map<String, Object> queryCriteria) throws IOException {
        CountRequest.Builder countBuilder = new CountRequest.Builder().index(index);
        countBuilder.query(q -> q.withJson(
                new java.io.ByteArrayInputStream(objectMapper.writeValueAsBytes(queryCriteria))
        ));

        CountResponse response = client.count(countBuilder.build());
        long count = response.count();
        log.info("Count for index={} query={}: {}", index, queryCriteria, count);
        return count;
    }

    /**
     * Fetches a <b>single batch</b> of documents from Elasticsearch using {@code search_after}
     * pagination.
     *
     * <p>This is the building-block for checkpoint-recoverable fetching: the caller (Flink
     * operator) stores the returned {@code nextSearchAfter} cursor in keyed state, so that after
     * a crash the operator can resume from the last checkpointed position instead of re-fetching
     * everything from scratch.</p>
     *
     * <p>Uses the <b>synchronous</b> ES client intentionally — each batch must complete and its
     * cursor must be persisted to Flink state before moving to the next batch.  An asynchronous
     * fetch would make it impossible to guarantee the cursor is checkpointed between batches.</p>
     *
     * @param slice       the slice to query
     * @param batchSize   max documents per batch
     * @param searchAfter the cursor from the previous batch, or {@code null} for the first page
     * @return batch result containing documents and the cursor for the next page (null if done)
     */
    @SuppressWarnings("unchecked")
    public FetchBatchResult fetchBatch(ReflowSlice slice, int batchSize,
                                       List<String> searchAfter) throws IOException {

        SearchRequest.Builder searchBuilder = new SearchRequest.Builder()
                .index(slice.getIndex())
                .size(batchSize)
                .sort(s -> s.field(f -> f.field("_id")));

        // Apply the query
        searchBuilder.query(q -> q.withJson(
                new java.io.ByteArrayInputStream(
                        objectMapper.writeValueAsBytes(slice.getQuery()))
        ));

        // Apply slicing if needed
        if (slice.isSliced()) {
            final int sId = slice.getSliceId();
            final int sMax = slice.getMaxSlices();
            searchBuilder.slice(sl -> sl.id(String.valueOf(sId)).max(sMax));
        }

        // Apply search_after cursor for pagination
        if (searchAfter != null) {
            searchBuilder.searchAfter(searchAfter.stream()
                    .map(v -> co.elastic.clients.json.JsonData.of(v))
                    .toList());
        }

        SearchResponse<Map> response = client.search(searchBuilder.build(), Map.class);
        List<Hit<Map>> hits = response.hits().hits();

        List<RawDocument> documents = new ArrayList<>();
        for (Hit<Map> hit : hits) {
            Map<String, Object> source = hit.source();
            if (source != null) {
                documents.add(RawDocument.builder()
                        .documentId(hit.id())
                        .payload(source)
                        .boomerangUpdateCount(extractUpdateCount(source))
                        .reflow(true)
                        .existingEnrichments(extractEnrichments(source))
                        .build());
            }
        }

        // Determine next cursor — null signals "no more pages"
        List<String> nextSearchAfter = null;
        if (!hits.isEmpty() && hits.size() >= batchSize) {
            Hit<Map> lastHit = hits.get(hits.size() - 1);
            if (lastHit.sort() != null && !lastHit.sort().isEmpty()) {
                nextSearchAfter = lastHit.sort().stream()
                        .map(Object::toString)
                        .toList();
            }
        }

        return new FetchBatchResult(documents, nextSearchAfter);
    }

    /**
     * Convenience method that fetches <b>all</b> documents matching a slice by repeatedly
     * calling {@link #fetchBatch}.  Useful for tests or non-Flink callers that do not need
     * checkpoint recovery.
     */
    public List<RawDocument> fetchDocuments(ReflowSlice slice, int batchSize) throws IOException {
        List<RawDocument> results = new ArrayList<>();
        List<String> searchAfter = null;

        while (true) {
            FetchBatchResult batch = fetchBatch(slice, batchSize, searchAfter);
            results.addAll(batch.getDocuments());

            if (!batch.hasMore()) {
                break;
            }
            searchAfter = batch.getNextSearchAfter();
        }

        log.info("Fetched {} documents for slice {}", results.size(), slice);
        return results;
    }

    @SuppressWarnings("unchecked")
    private long extractUpdateCount(Map<String, Object> source) {
        Object count = source.get("boomerangUpdateCount");
        if (count instanceof Number) {
            return ((Number) count).longValue();
        }
        return 0L;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Map<String, Object>> extractEnrichments(Map<String, Object> source) {
        Object enrichments = source.get("enrichments");
        if (enrichments instanceof Map) {
            return (Map<String, Map<String, Object>>) enrichments;
        }
        return Collections.emptyMap();
    }

    @Override
    public void close() throws IOException {
        if (restClient != null) {
            restClient.close();
        }
    }
}
