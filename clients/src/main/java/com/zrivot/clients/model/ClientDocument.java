package com.zrivot.clients.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Domain model for a client document.
 *
 * <p>This is a typed convenience wrapper around the generic {@code Map<String, Object>}
 * payload that flows through the pipeline. Use it to build or read payloads in a
 * type-safe way while the underlying pipeline remains schema-agnostic.</p>
 *
 * <p>Example – creating a raw document payload from a client:
 * <pre>
 *   ClientDocument client = ClientDocument.builder()
 *       .clientId("CLI-001")
 *       .firstName("John")
 *       .lastName("Doe")
 *       .email("john@example.com")
 *       .build();
 *   Map&lt;String, Object&gt; payload = client.toPayload();
 * </pre>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ClientDocument implements Serializable {

    private static final long serialVersionUID = 1L;

    private String clientId;
    private String firstName;
    private String lastName;
    private String email;
    private String phone;
    private String address;
    private String city;
    private String country;
    private String registrationDate;
    private String tier;

    /**
     * Converts this typed client into a generic payload map that can be
     * wrapped in a {@link com.zrivot.model.RawDocument}.
     */
    public Map<String, Object> toPayload() {
        Map<String, Object> map = new HashMap<>();
        putIfNotNull(map, "clientId", clientId);
        putIfNotNull(map, "firstName", firstName);
        putIfNotNull(map, "lastName", lastName);
        putIfNotNull(map, "email", email);
        putIfNotNull(map, "phone", phone);
        putIfNotNull(map, "address", address);
        putIfNotNull(map, "city", city);
        putIfNotNull(map, "country", country);
        putIfNotNull(map, "registrationDate", registrationDate);
        putIfNotNull(map, "tier", tier);
        return map;
    }

    /**
     * Creates a typed {@code ClientDocument} from a generic payload map.
     */
    public static ClientDocument fromPayload(Map<String, Object> payload) {
        if (payload == null) return new ClientDocument();
        return ClientDocument.builder()
                .clientId((String) payload.get("clientId"))
                .firstName((String) payload.get("firstName"))
                .lastName((String) payload.get("lastName"))
                .email((String) payload.get("email"))
                .phone((String) payload.get("phone"))
                .address((String) payload.get("address"))
                .city((String) payload.get("city"))
                .country((String) payload.get("country"))
                .registrationDate((String) payload.get("registrationDate"))
                .tier((String) payload.get("tier"))
                .build();
    }

    private static void putIfNotNull(Map<String, Object> map, String key, Object value) {
        if (value != null) {
            map.put(key, value);
        }
    }
}
