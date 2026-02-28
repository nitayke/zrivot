package com.zrivot.purchases.enrichment;

import com.zrivot.enrichment.Enricher.ApiResponseMapper;
import java.util.Map;

public class PurchaseApiResponseMapper implements ApiResponseMapper {
    @Override
    public Map<String, Object> mapFromResponse(String documentId, Map<String, Object> payload, Map<String, Object> apiResponse) {
        // Complex logic for API response mapping
        return Map.of(
            "geo_latitude", apiResponse.get("lat"),
            "geo_longitude", apiResponse.get("lon"),
            "geo_region", apiResponse.get("region")
        );
    }
}
