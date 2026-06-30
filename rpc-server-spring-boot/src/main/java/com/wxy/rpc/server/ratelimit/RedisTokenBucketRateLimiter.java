package com.wxy.rpc.server.ratelimit;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 基于本地预取 + Redis 全局令牌桶的分布式限流器。
 *
 * 多个 provider 使用同一个 Redis key 共享全局令牌桶状态，从而控制整个服务集群的总 QPS。
 * provider 会优先扣减本地预取的令牌，本地令牌不足时再通过 Lua 从 Redis 全局令牌桶批量申请令牌。
 * 这种方式可以显著减少 Redis 访问次数，适合高 QPS 且允许少量误差的场景。
 * Lua 脚本保证全局令牌的补充、判断和批量扣减在 Redis 内部原子执行。
 */
@Slf4j
public class RedisTokenBucketRateLimiter implements RateLimiter {

    private static final String TOKEN_BUCKET_SCRIPT =
            "local key = KEYS[1]\n" +
            "local now = tonumber(ARGV[1])\n" +
            "local refillTokens = tonumber(ARGV[2])\n" +
            "local refillIntervalMillis = tonumber(ARGV[3])\n" +
            "local bucketCapacity = tonumber(ARGV[4])\n" +
            "local requestTokens = tonumber(ARGV[5])\n" +
            "local ttlSeconds = tonumber(ARGV[6])\n" +
            "local tokens = tonumber(redis.call('HGET', key, 'tokens'))\n" +
            "local lastRefillTime = tonumber(redis.call('HGET', key, 'lastRefillTime'))\n" +
            "if tokens == nil then\n" +
            "  tokens = bucketCapacity\n" +
            "  lastRefillTime = now\n" +
            "end\n" +
            "local elapsed = now - lastRefillTime\n" +
            "if elapsed >= refillIntervalMillis then\n" +
            "  local intervals = math.floor(elapsed / refillIntervalMillis)\n" +
            "  local tokensToAdd = intervals * refillTokens\n" +
            "  tokens = math.min(bucketCapacity, tokens + tokensToAdd)\n" +
            "  lastRefillTime = lastRefillTime + intervals * refillIntervalMillis\n" +
            "end\n" +
            "requestTokens = math.min(requestTokens, bucketCapacity)\n" +
            "local acquiredTokens = math.min(tokens, requestTokens)\n" +
            "if acquiredTokens <= 0 then\n" +
            "  redis.call('HMSET', key, 'tokens', tokens, 'lastRefillTime', lastRefillTime)\n" +
            "  redis.call('EXPIRE', key, ttlSeconds)\n" +
            "  return 0\n" +
            "end\n" +
            "tokens = tokens - acquiredTokens\n" +
            "redis.call('HMSET', key, 'tokens', tokens, 'lastRefillTime', lastRefillTime)\n" +
            "redis.call('EXPIRE', key, ttlSeconds)\n" +
            "return acquiredTokens\n";

    private final int refillTokens;

    private final long refillIntervalMillis;

    private final int bucketCapacity;

    private final String keyPrefix;

    private final int redisBatchSize;

    private final boolean fallbackToLocal;

    private final boolean failOpen;

    private final RateLimiter localFallbackRateLimiter;

    private final JedisPool jedisPool;

    private final ConcurrentMap<String, LocalBucket> localBuckets = new ConcurrentHashMap<>();

    public RedisTokenBucketRateLimiter(int refillTokens, long refillIntervalMillis, int bucketCapacity,
                                       String redisHost, int redisPort, String redisPassword, int redisDatabase,
                                       int redisTimeoutMillis, String keyPrefix, int redisBatchSize,
                                       boolean fallbackToLocal, boolean failOpen,
                                       RateLimiter localFallbackRateLimiter) {
        this.refillTokens = Math.max(refillTokens, 1);
        this.refillIntervalMillis = Math.max(refillIntervalMillis, 1L);
        this.bucketCapacity = Math.max(bucketCapacity, this.refillTokens);
        this.keyPrefix = keyPrefix == null || keyPrefix.trim().isEmpty()
                ? "wxy-rpc:rate-limit:" : keyPrefix.trim();
        this.redisBatchSize = Math.min(Math.max(redisBatchSize, 1), this.bucketCapacity);
        this.fallbackToLocal = fallbackToLocal;
        this.failOpen = failOpen;
        this.localFallbackRateLimiter = localFallbackRateLimiter;
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(32);
        poolConfig.setMaxIdle(8);
        poolConfig.setMinIdle(0);
        this.jedisPool = new JedisPool(poolConfig, redisHost, redisPort, Math.max(redisTimeoutMillis, 1),
                emptyToNull(redisPassword), redisDatabase);
    }

    @Override
    public boolean tryAcquire(String resource) {
        LocalBucket localBucket = localBuckets.computeIfAbsent(resource, key -> new LocalBucket());
        synchronized (localBucket) {
            if (localBucket.tokens > 0) {
                localBucket.tokens--;
                return true;
            }
            int acquiredTokens = acquireTokensFromRedis(resource);
            if (acquiredTokens <= 0) {
                return false;
            }
            localBucket.tokens = acquiredTokens - 1;
            return true;
        }
    }

    private int acquireTokensFromRedis(String resource) {
        String key = keyPrefix + resource;
        List<String> keys = Collections.singletonList(key);
        List<String> args = Arrays.asList(
                String.valueOf(System.currentTimeMillis()),
                String.valueOf(refillTokens),
                String.valueOf(refillIntervalMillis),
                String.valueOf(bucketCapacity),
                String.valueOf(redisBatchSize),
                String.valueOf(getTtlSeconds())
        );
        try (Jedis jedis = jedisPool.getResource()) {
            Object result = jedis.eval(TOKEN_BUCKET_SCRIPT, keys, args);
            if (result instanceof Number) {
                return ((Number) result).intValue();
            }
            return 0;
        } catch (Exception e) {
            log.warn("Redis token bucket rate limiter failed, resource: {}, fallbackToLocal: {}, failOpen: {}.",
                    resource, fallbackToLocal, failOpen);
            log.debug("Redis token bucket failure detail: {}", e.toString());
            if (fallbackToLocal && localFallbackRateLimiter != null) {
                return localFallbackRateLimiter.tryAcquire(resource) ? 1 : 0;
            }
            return failOpen ? 1 : 0;
        }
    }

    private long getTtlSeconds() {
        long refillIntervalsToFull = bucketCapacity / refillTokens + 2L;
        long ttlMillis = Math.max(60_000L, refillIntervalsToFull * refillIntervalMillis);
        return Math.max(ttlMillis / 1000L, 1L);
    }

    private static String emptyToNull(String value) {
        return value == null || value.trim().isEmpty() ? null : value;
    }

    private static class LocalBucket {
        private int tokens;
    }
}
