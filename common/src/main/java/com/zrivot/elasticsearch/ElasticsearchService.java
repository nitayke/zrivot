package com.zrivot.elasticsearch;

import com.zrivot.config.ElasticsearchConfig;
import com.zrivot.model.RawDocument;
import com.zrivot.model.ReflowSlice;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
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

import java.io.ByteArrayInputStream;
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
 *   <li>Fetching documents in batches using search_after, with configurable sort</li>
 *   <li>Re-fetching individual documents by ID (for retry after enrichment failure)</li>
 * </ul>
 *
 * <p>Instances are created per-operator and are NOT serialisable across Flink checkpoints;
 * they should be initialised inside {@code open()} methods.</p>
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
        byte[] queryBytes = objectMapper.writeValueAsBytes(queryCriteria);

        CountRequest.Builder countBuilder = new CountRequest.Builder().index(index);
        countBuilder.query(q -> q.withJson(new ByteArrayInputStream(queryBytes)));

        CountResponse response = client.count(countBuilder.build());
        long count = response.count();
        log.info("Count for index={} query={}: {}", index, queryCriteria, count);
        return count;
    }

    /**
     * Fetches a <b>single batch</b> of documents from Elasticsearch using {@code search_after}
     * pagination.
     *
     * <p>Supports a configurable sort field: when {@code sortField} is non-null, documents
     * are sorted by that field descending (newest first), then by {@code _id}.  When null,
     * documents are sorted by {@code _id} only.</p>
     *
     * @param slice       the slice to query
     * @param batchSize   max documents per batch
     * @param searchAfter the cursor from the previous batch, or {@code null} for the first page
     * @param sortField   optional time field (e.g. "startTime") for descending sort; null for _id only
     * @return batch result containing documents and the cursor for the next page (null if done)
     */
    @SuppressWarnings("unchecked")
    public FetchBatchResult fetchBatch(ReflowSlice slice, int batchSize,
                                       List<String> searchAfter,
                                       String sortField) throws IOException {

        SearchRequest.Builder searchBuilder = new SearchRequest.Builder()
                .index(slice.getIndex())
                .size(batchSize);

        // Sort: optional time field DESC, then _id ASC
        if (sortField != null && !sortField.isBlank()) {
            searchBuilder.sort(s -> s.field(f -> f.field(sortField).order(co.elastic.clients.elasticsearch._types.SortOrder.Desc)));
        }
        searchBuilder.sort(s -> s.field(f -> f.field("_id").order(co.elastic.clients.elasticsearch._types.SortOrder.Asc)));

        // Apply the query — serialize outside lambda to avoid checked exception in lambda
        byte[] queryBytes = objectMapper.writeValueAsBytes(slice.getQuery());
        searchBuilder.query(q -> q.withJson(new ByteArrayInputStream(queryBytes)));

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
     * calling {@link #fetchBatch}.
     */
    public List<RawDocument> fetchDocuments(ReflowSlice slice, int batchSize,
                                            String sortField) throws IOException {
        List<RawDocument> results = new ArrayList<>();
        List<String> searchAfter = null;

        while (true) {
            FetchBatchResult batch = fetchBatch(slice, batchSize, searchAfter, sortField);
            results.addAll(batch.getDocuments());

            if (!batch.hasMore()) {
                break;
            }
            searchAfter = batch.getNextSearchAfter();
        }

        log.info("Fetched {} documents for slice {}", results.size(), slice);
        return results;
    }

    /**
     * Re-fetches documents by their IDs from Elasticsearch.
     * Used for retry-with-refetch when enrichment fails on reflow documents.
     *
     * @param index       the ES index
     * @param documentIds list of document IDs to fetch
     * @return list of RawDocuments with fresh data from ES
     */
    @SuppressWarnings("unchecked")
    public List<RawDocument> fetchByIds(String index, List<String> documentIds) throws IOException {
        if (documentIds == null || documentIds.isEmpty()) {
            return Collections.emptyList();
        }

        SearchRequest request = SearchRequest.of(s -> s
                .index(index)
                .size(documentIds.size())
                .query(q -> q.ids(i -> i.values(documentIds)))
        );

        SearchResponse<Map> response = client.search(request, Map.class);
        List<RawDocument> documents = new ArrayList<>();

        for (Hit<Map> hit : response.hits().hits()) {
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

        return documents;
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
