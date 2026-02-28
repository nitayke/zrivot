package com.zrivot.purchases.enrichment;

import com.zrivot.enrichment.Enricher.ApiRequestMapper;
import java.util.Map;

public class PurchaseApiRequestMapper implements ApiRequestMapper {
    @Override
    public Map<String, Object> mapToRequest(String documentId, Map<String, Object> payload) {
        // Complex logic for API request mapping
        String storeId = (String) payload.get("storeId");
        String currency = (String) payload.get("currency");
        return Map.of("store_id", storeId, "currency_code", currency);
    }
}
