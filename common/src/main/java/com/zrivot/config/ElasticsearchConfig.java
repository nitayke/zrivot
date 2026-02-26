package com.zrivot.config;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

/**
 * Elasticsearch cluster connection configuration.
 */
@Data
@NoArgsConstructor
public class ElasticsearchConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private List<String> hosts;
    private String index;
    private String username;
    private String password;
    private int connectTimeoutMs = 5000;
    private int socketTimeoutMs = 30000;
}
