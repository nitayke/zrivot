package com.zrivot.purchases.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 * Domain model for a purchase document.
 *
 * <p>This is a typed convenience wrapper around the generic {@code Map<String, Object>}
 * payload that flows through the pipeline. Use it to build or read payloads in a
 * type-safe way while the underlying pipeline remains schema-agnostic.</p>
 *
 * <p>Example – creating a raw document payload from a purchase:
 * <pre>
 *   PurchaseDocument purchase = PurchaseDocument.builder()
 *       .purchaseId("PUR-001")
 *       .customerId("CUST-42")
 *       .amount(new BigDecimal("129.99"))
 *       .currency("USD")
 *       .productId("PROD-7")
 *       .build();
 *   Map&lt;String, Object&gt; payload = purchase.toPayload();
 * </pre>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PurchaseDocument implements Serializable {

    private static final long serialVersionUID = 1L;

    private String purchaseId;
    private String customerId;
    private BigDecimal amount;
    private String currency;
    private String productId;
    private String productCategory;
    private String purchaseDate;
    private String storeId;
    private String paymentMethod;

    /**
     * Converts this typed purchase into a generic payload map that can be
     * wrapped in a {@link com.zrivot.model.RawDocument}.
     */
    public Map<String, Object> toPayload() {
        Map<String, Object> map = new HashMap<>();
        putIfNotNull(map, "purchaseId", purchaseId);
        putIfNotNull(map, "customerId", customerId);
        putIfNotNull(map, "amount", amount);
        putIfNotNull(map, "currency", currency);
        putIfNotNull(map, "productId", productId);
        putIfNotNull(map, "productCategory", productCategory);
        putIfNotNull(map, "purchaseDate", purchaseDate);
        putIfNotNull(map, "storeId", storeId);
        putIfNotNull(map, "paymentMethod", paymentMethod);
        return map;
    }

    /**
     * Creates a typed {@code PurchaseDocument} from a generic payload map.
     */
    public static PurchaseDocument fromPayload(Map<String, Object> payload) {
        if (payload == null) return new PurchaseDocument();
        return PurchaseDocument.builder()
                .purchaseId((String) payload.get("purchaseId"))
                .customerId((String) payload.get("customerId"))
                .amount(payload.get("amount") != null
                        ? new BigDecimal(payload.get("amount").toString()) : null)
                .currency((String) payload.get("currency"))
                .productId((String) payload.get("productId"))
                .productCategory((String) payload.get("productCategory"))
                .purchaseDate((String) payload.get("purchaseDate"))
                .storeId((String) payload.get("storeId"))
                .paymentMethod((String) payload.get("paymentMethod"))
                .build();
    }

    private static void putIfNotNull(Map<String, Object> map, String key, Object value) {
        if (value != null) {
            map.put(key, value);
        }
    }
}
