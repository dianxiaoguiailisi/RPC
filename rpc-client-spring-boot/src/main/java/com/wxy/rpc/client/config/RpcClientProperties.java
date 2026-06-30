package com.wxy.rpc.client.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Rpc Client 配置属性类
 *
 * @author Wuxy
 * @version 1.0
 * @ClassName RpcClientProperties
 * @Date 2023/1/7 15:12
 */
@Data
@ConfigurationProperties(prefix = "rpc.client")
public class RpcClientProperties {

    /**
     * Load balancing algorithm, candidate values include: (random, roundRobin, consistentHash, adaptive),
     * the default is random.
     */
    private String loadbalance;

    /**
     * Serialization algorithm, candidate values include: (JDK, JSON, HESSIAN, KRYO, PROTOSTUFF), default: HESSIAN
     */
    private String serialization;

    /**
     * Communication protocols, such as netty and http, are netty by default
     */
    private String transport;

    /**
     * Registration center, such as (zookeeper, nacos, etc.), defaults to: zookeeper
     */
    private String registry;

    /**
     * Service discovery (registry) address. The default is "127.0.0.1:2181"
     */
    private String registryAddr;

    /**
     * Connection timeout, default: 5000
     */
    private Integer timeout;

    /**
     * Failover retry count, default: 0.
     *
     * A value of 0 means only one call attempt.
     * A value of 2 means one first call plus two retry attempts.
     */
    private Integer retries;

    /**
     * Provider isolation threshold, default: 3.
     *
     * When a provider fails continuously and reaches this threshold,
     * it will be temporarily isolated by the client.
     */
    private Integer failureThreshold;

    /**
     * Provider circuit open duration in milliseconds, default: 10000.
     *
     * During this period, the client will skip the failed provider.
     * After the duration expires, one request can be used as a half-open probe.
     */
    private Long circuitOpenMillis;

    /**
     * Whether to retry timeout exceptions, default: true.
     */
    private Boolean retryOnTimeout;

    /**
     * Whether to retry connection exceptions, default: true.
     */
    private Boolean retryOnConnectFailure;

    /**
     * Whether to retry remote business exceptions, default: false.
     */
    private Boolean retryOnBusinessException;

    /**
     * Slow call threshold in milliseconds, default: 1000.
     *
     * A call whose elapsed time is greater than this value will be recorded as a slow call.
     */
    private Long slowCallThresholdMillis;

    /**
     * Slow call threshold count, default: 3.
     *
     * When a provider reaches this count continuously, it will be temporarily isolated.
     */
    private Integer slowCallThresholdCount;

    public RpcClientProperties() {
        this.loadbalance = "random";
        this.serialization = "HESSIAN";
        this.transport = "netty";
        this.registry = "zookeeper";
        this.registryAddr = "127.0.0.1:2181";
        this.timeout = 5000;
        this.retries = 0;
        this.failureThreshold = 3;
        this.circuitOpenMillis = 10000L;
        this.retryOnTimeout = true;
        this.retryOnConnectFailure = true;
        this.retryOnBusinessException = false;
        this.slowCallThresholdMillis = 1000L;
        this.slowCallThresholdCount = 3;
    }
}
