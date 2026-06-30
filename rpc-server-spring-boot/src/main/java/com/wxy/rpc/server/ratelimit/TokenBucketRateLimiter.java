package com.wxy.rpc.server.ratelimit;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 令牌桶限流器。
 *
 * 桶中按照固定速率生成令牌，请求到来时消耗一个令牌。
 * 如果桶中没有令牌，请求会被限流。
 *
 * 令牌桶允许一定程度的突发流量，突发上限由 bucketCapacity 控制。
 */
public class TokenBucketRateLimiter implements RateLimiter {

    private final int refillTokens;

    private final long refillIntervalMillis;

    private final int bucketCapacity;

    private final ConcurrentMap<String, Bucket> buckets = new ConcurrentHashMap<>();

    public TokenBucketRateLimiter(int refillTokens, long refillIntervalMillis, int bucketCapacity) {
        this.refillTokens = Math.max(refillTokens, 1);
        this.refillIntervalMillis = Math.max(refillIntervalMillis, 1L);
        this.bucketCapacity = Math.max(bucketCapacity, this.refillTokens);
    }

    @Override
    public boolean tryAcquire(String resource) {
        Bucket bucket = buckets.computeIfAbsent(resource, key -> new Bucket(bucketCapacity));
        synchronized (bucket) {
            refill(bucket);
            if (bucket.tokens <= 0) {
                return false;
            }
            bucket.tokens--;
            return true;
        }
    }

    private void refill(Bucket bucket) {
        long now = System.currentTimeMillis();
        long elapsed = now - bucket.lastRefillTime;
        if (elapsed < refillIntervalMillis) {
            return;
        }
        long intervals = elapsed / refillIntervalMillis;
        int tokensToAdd = (int) Math.min((long) bucketCapacity, intervals * refillTokens);
        bucket.tokens = Math.min(bucketCapacity, bucket.tokens + tokensToAdd);
        bucket.lastRefillTime += intervals * refillIntervalMillis;
    }

    private static class Bucket {
        private int tokens;
        private long lastRefillTime = System.currentTimeMillis();

        private Bucket(int bucketCapacity) {
            this.tokens = bucketCapacity;
        }
    }
}
