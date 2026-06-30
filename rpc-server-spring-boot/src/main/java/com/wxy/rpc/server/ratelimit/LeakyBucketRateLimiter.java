package com.wxy.rpc.server.ratelimit;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 漏桶限流器。
 *
 * 请求进入桶中排队，桶按照固定速率漏水。
 * 如果桶已经满了，新请求会被限流。
 *
 * 和令牌桶相比，漏桶更强调平滑输出，对突发流量更严格。
 */
public class LeakyBucketRateLimiter implements RateLimiter {

    private final int leakRequests;

    private final long leakIntervalMillis;

    private final int bucketCapacity;

    private final ConcurrentMap<String, Bucket> buckets = new ConcurrentHashMap<>();

    public LeakyBucketRateLimiter(int leakRequests, long leakIntervalMillis, int bucketCapacity) {
        this.leakRequests = Math.max(leakRequests, 1);
        this.leakIntervalMillis = Math.max(leakIntervalMillis, 1L);
        this.bucketCapacity = Math.max(bucketCapacity, this.leakRequests);
    }

    @Override
    public boolean tryAcquire(String resource) {
        Bucket bucket = buckets.computeIfAbsent(resource, key -> new Bucket());
        synchronized (bucket) {
            leak(bucket);
            if (bucket.water >= bucketCapacity) {
                return false;
            }
            bucket.water++;
            return true;
        }
    }

    private void leak(Bucket bucket) {
        long now = System.currentTimeMillis();
        long elapsed = now - bucket.lastLeakTime;
        if (elapsed < leakIntervalMillis) {
            return;
        }
        long intervals = elapsed / leakIntervalMillis;
        int leaked = (int) Math.min((long) bucket.water, intervals * leakRequests);
        bucket.water -= leaked;
        bucket.lastLeakTime += intervals * leakIntervalMillis;
    }

    private static class Bucket {
        private int water;
        private long lastLeakTime = System.currentTimeMillis();
    }
}
