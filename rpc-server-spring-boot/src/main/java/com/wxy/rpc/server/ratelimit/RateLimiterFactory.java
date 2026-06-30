package com.wxy.rpc.server.ratelimit;

import com.wxy.rpc.server.config.RpcServerProperties;

/**
 * 服务端限流器工厂。
 *
 * RpcRequestHandler 不是由 Spring 直接创建，而是通过 SingletonFactory 获取，
 * 所以这里使用静态工厂保存当前服务端限流器实例。
 */
public class RateLimiterFactory {

    private static volatile RateLimiter rateLimiter = new NoopRateLimiter();

    private RateLimiterFactory() {
    }

    /**
     * 根据服务端配置初始化限流器。
     *
     * @param properties 服务端配置
     */
    public static void init(RpcServerProperties properties) {
        if (!Boolean.TRUE.equals(properties.getRateLimitEnabled())) {
            rateLimiter = new NoopRateLimiter();
            return;
        }
        String type = properties.getRateLimitType();
        int permits = properties.getRateLimitPermits();
        long windowMillis = properties.getRateLimitWindowMillis();
        int capacity = properties.getRateLimitBucketCapacity();
        int redisBatchSize = properties.getRateLimitRedisBatchSize() == null
                ? capacity : properties.getRateLimitRedisBatchSize();
        if ("fixedWindow".equalsIgnoreCase(type)) {
            rateLimiter = new FixedWindowRateLimiter(permits, windowMillis);
        } else if ("slidingWindow".equalsIgnoreCase(type)) {
            rateLimiter = new SlidingWindowRateLimiter(permits, windowMillis);
        } else if ("tokenBucket".equalsIgnoreCase(type)) {
            rateLimiter = new TokenBucketRateLimiter(permits, windowMillis, capacity);
        } else if ("leakyBucket".equalsIgnoreCase(type)) {
            rateLimiter = new LeakyBucketRateLimiter(permits, windowMillis, capacity);
        } else if ("redisTokenBucket".equalsIgnoreCase(type)) {
            RateLimiter localFallbackRateLimiter = new TokenBucketRateLimiter(permits, windowMillis, capacity);
            rateLimiter = new RedisTokenBucketRateLimiter(permits, windowMillis, capacity,
                    properties.getRateLimitRedisHost(), properties.getRateLimitRedisPort(),
                    properties.getRateLimitRedisPassword(), properties.getRateLimitRedisDatabase(),
                    properties.getRateLimitRedisTimeoutMillis(), properties.getRateLimitRedisKeyPrefix(),
                    redisBatchSize,
                    Boolean.TRUE.equals(properties.getRateLimitRedisFallbackToLocal()),
                    Boolean.TRUE.equals(properties.getRateLimitRedisFailOpen()), localFallbackRateLimiter);
        } else {
            throw new IllegalArgumentException("Unsupported rate limit type: " + type);
        }
    }

    /**
     * 获取当前限流器。
     *
     * @return 当前限流器
     */
    public static RateLimiter getRateLimiter() {
        return rateLimiter;
    }
}
