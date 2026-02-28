package com.zrivot.purchases.enrichment;

import com.zrivot.config.EnricherConfig;
import com.zrivot.enrichment.Enricher;

import java.util.Map;
import com.zrivot.purchases.enrichment.PurchaseQueryMapper;
import com.zrivot.purchases.enrichment.PurchaseApiRequestMapper;
import com.zrivot.purchases.enrichment.PurchaseApiResponseMapper;
import com.zrivot.purchases.enrichment.PurchaseFieldMapper;

public class PurchaseEnricher implements Enricher {
    private EnricherConfig config;
    private QueryMapper queryMapper;
    private ApiRequestMapper requestMapper;
    private ApiResponseMapper responseMapper;
    private FieldMapper fieldMapper;

    @Override
    public void init(EnricherConfig config) {
        this.config = config;
        this.queryMapper = new PurchaseQueryMapper();
        this.requestMapper = new PurchaseApiRequestMapper();
        this.responseMapper = new PurchaseApiResponseMapper();
        this.fieldMapper = new PurchaseFieldMapper();
    }

    @Override
    public QueryMapper getQueryMapper() {
        return queryMapper;
    }

    @Override
    public ApiRequestMapper getApiRequestMapper() {
        return requestMapper;
    }

    @Override
    public ApiResponseMapper getApiResponseMapper() {
        return responseMapper;
    }

    @Override
    public FieldMapper getFieldMapper() {
        return fieldMapper;
    }

    @Override
    public Map<String, Object> enrich(String documentId, Map<String, Object> payload) throws Exception {
        Map<String, Object> apiRequest = requestMapper.mapToRequest(documentId, payload);
        // ... call API (omitted for brevity) ...
        Map<String, Object> apiResponse = Map.of(); // Replace with actual API response
        Map<String, Object> enriched = responseMapper.mapFromResponse(documentId, payload, apiResponse);
        return PurchaseFieldMapper.applyFieldMapping(enriched, fieldMapper);
    }

    @Override
    public void close() {
        // Cleanup resources if needed
    }
}
