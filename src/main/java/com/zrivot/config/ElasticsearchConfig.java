package com.zrivot.config;

import java.io.Serializable;
import java.util.List;

/**
 * Elasticsearch cluster connection configuration.
 */
public class ElasticsearchConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private List<String> hosts;
    private String index;
    private String username;
    private String password;
    private int connectTimeoutMs = 5000;
    private int socketTimeoutMs = 30000;

    public ElasticsearchConfig() {
    }

    public List<String> getHosts() {
        return hosts;
    }

    public void setHosts(List<String> hosts) {
        this.hosts = hosts;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getConnectTimeoutMs() {
        return connectTimeoutMs;
    }

    public void setConnectTimeoutMs(int connectTimeoutMs) {
        this.connectTimeoutMs = connectTimeoutMs;
    }

    public int getSocketTimeoutMs() {
        return socketTimeoutMs;
    }

    public void setSocketTimeoutMs(int socketTimeoutMs) {
        this.socketTimeoutMs = socketTimeoutMs;
    }
}
