package com.zrivot.purchases.enrichment;

import com.zrivot.enrichment.Enricher.FieldMapper;
import java.util.Map;
import java.util.stream.Collectors;

public class PurchaseFieldMapper implements FieldMapper {
    @Override
    public String mapField(String logicalFieldName) {
        return switch (logicalFieldName) {
            case "storeId" -> "store_id";
            case "currency" -> "currency_code";
            case "geoLatitude" -> "geo_latitude";
            case "geoLongitude" -> "geo_longitude";
            case "geoRegion" -> "geo_region";
            default -> logicalFieldName;
        };
    }

    public static Map<String, Object> applyFieldMapping(Map<String, Object> input, FieldMapper mapper) {
        return input.entrySet().stream()
            .collect(Collectors.toMap(
                e -> mapper.mapField(e.getKey()),
                Map.Entry::getValue
            ));
    }
}
