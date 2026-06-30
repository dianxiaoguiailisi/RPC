package com.wxy.rpc.server.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * 服务端配置属性类（必须提供 getter、setter 方法，否则无法注入属性值）
 *
 * @author Wuxy
 * @version 1.0
 * @ClassName RpcServerProperties
 * @Date 2023/1/6 23:33
 */
@Data
@ConfigurationProperties(prefix = "rpc.server")
public class RpcServerProperties {

    /**
     * The service provider address, default is {@link InetAddress#getHostAddress()}.
     */
    private String address;

    /**
     * The service startup port is 8080 by default
     */
    private Integer port;

    /**
     * Application name, which defaults to provider-1
     */
    private String appName;

    /**
     * Registration center, such as zookeeper and nacos, defaults to zookeeper
     */
    private String registry;

    /**
     * Transmission protocols, such as netty, http or socket etc..., are netty by default
     */
    private String transport;

    /**
     * The address of the registry is 127.0.0.1:2181 by default
     */
    private String registryAddr;

    /**
     * Whether server side rate limiting is enabled.
     */
    private Boolean rateLimitEnabled;

    /**
     * Rate limit algorithm.
     *
     * Candidate values: fixedWindow, slidingWindow, tokenBucket, leakyBucket, redisTokenBucket.
     */
    private String rateLimitType;

    /**
     * Rate limit permits in one window or refill/leak interval.
     */
    private Integer rateLimitPermits;

    /**
     * Rate limit window/refill/leak interval in milliseconds.
     */
    private Long rateLimitWindowMillis;

    /**
     * Bucket capacity for tokenBucket and leakyBucket.
     */
    private Integer rateLimitBucketCapacity;

    /**
     * Redis host for distributed token bucket.
     */
    private String rateLimitRedisHost;

    /**
     * Redis port for distributed token bucket.
     */
    private Integer rateLimitRedisPort;

    /**
     * Redis password for distributed token bucket.
     */
    private String rateLimitRedisPassword;

    /**
     * Redis database for distributed token bucket.
     */
    private Integer rateLimitRedisDatabase;

    /**
     * Redis command timeout in milliseconds.
     */
    private Integer rateLimitRedisTimeoutMillis;

    /**
     * Redis key prefix for distributed token bucket.
     */
    private String rateLimitRedisKeyPrefix;

    /**
     * Number of tokens requested from Redis in one batch.
     */
    private Integer rateLimitRedisBatchSize;

    /**
     * Whether to fallback to local token bucket when Redis is unavailable.
     */
    private Boolean rateLimitRedisFallbackToLocal;

    /**
     * Whether requests are allowed when Redis is unavailable and local fallback is disabled.
     */
    private Boolean rateLimitRedisFailOpen;

    /**
     * Default business thread pool core size.
     */
    private Integer bizCorePoolSize;

    /**
     * Default business thread pool max size.
     */
    private Integer bizMaxPoolSize;

    /**
     * Default business thread pool queue capacity.
     */
    private Integer bizQueueCapacity;

    /**
     * Default business thread pool keep alive seconds.
     */
    private Long bizKeepAliveSeconds;

    /**
     * Whether service-level business thread pool isolation is enabled.
     */
    private Boolean bizIsolationEnabled;

    /**
     * Service-level isolated business thread pools.
     */
    private List<ServiceThreadPoolProperties> bizIsolationServices;

    /**
     * 进行默认初始化值
     */
    public RpcServerProperties() throws UnknownHostException {
        this.address = InetAddress.getLocalHost().getHostAddress();
        this.port = 8080;
        this.appName = "provider-1";
        this.registry = "zookeeper";
        this.transport = "netty";
        this.registryAddr = "127.0.0.1:2181";
        this.rateLimitEnabled = false;
        this.rateLimitType = "tokenBucket";
        this.rateLimitPermits = 100;
        this.rateLimitWindowMillis = 1000L;
        this.rateLimitBucketCapacity = 100;
        this.rateLimitRedisHost = "127.0.0.1";
        this.rateLimitRedisPort = 6379;
        this.rateLimitRedisPassword = "";
        this.rateLimitRedisDatabase = 0;
        this.rateLimitRedisTimeoutMillis = 2000;
        this.rateLimitRedisKeyPrefix = "wxy-rpc:rate-limit:";
        this.rateLimitRedisBatchSize = 100;
        this.rateLimitRedisFallbackToLocal = true;
        this.rateLimitRedisFailOpen = true;
        this.bizCorePoolSize = 10;
        this.bizMaxPoolSize = 10;
        this.bizQueueCapacity = 10000;
        this.bizKeepAliveSeconds = 60L;
        this.bizIsolationEnabled = false;
        this.bizIsolationServices = new ArrayList<>();
    }

    /**
     * Service-level business thread pool config.
     */
    @Data
    public static class ServiceThreadPoolProperties {
        /**
         * Service name generated by ServiceUtil.serviceKey(interfaceName, version).
         */
        private String serviceName;

        /**
         * Core pool size.
         */
        private Integer corePoolSize;

        /**
         * Max pool size.
         */
        private Integer maxPoolSize;

        /**
         * Queue capacity.
         */
        private Integer queueCapacity;

        /**
         * Keep alive seconds.
         */
        private Long keepAliveSeconds;
    }
}
