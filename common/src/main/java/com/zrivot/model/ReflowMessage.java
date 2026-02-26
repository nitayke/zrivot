package com.zrivot.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Map;

/**
 * Message from a reflow Kafka topic indicating "what changed" for a specific enricher.
 * The query criteria are translated into an Elasticsearch query to find affected documents.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ReflowMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    private String enricherName;
    private Map<String, Object> queryCriteria;
    private long timestamp;
}
