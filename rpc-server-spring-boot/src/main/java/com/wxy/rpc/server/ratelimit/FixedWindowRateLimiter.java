package com.wxy.rpc.server.ratelimit;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 固定窗口限流器。
 *
 * 在固定时间窗口内限制最大请求数。例如窗口 1000ms、阈值 100，
 * 表示每个 serviceName 每秒最多放行 100 个请求。
 */
public class FixedWindowRateLimiter implements RateLimiter {

    private final int permits;

    private final long windowMillis;

    private final ConcurrentMap<String, WindowCounter> counters = new ConcurrentHashMap<>();

    public FixedWindowRateLimiter(int permits, long windowMillis) {
        this.permits = Math.max(permits, 1);
        this.windowMillis = Math.max(windowMillis, 1L);
    }

    @Override
    public boolean tryAcquire(String resource) {
        WindowCounter counter = counters.computeIfAbsent(resource, key -> new WindowCounter());
        synchronized (counter) {
            long now = System.currentTimeMillis();
            if (now - counter.windowStart >= windowMillis) {
                counter.windowStart = now;
                counter.count = 0;
            }
            if (counter.count >= permits) {
                return false;
            }
            counter.count++;
            return true;
        }
    }

    private static class WindowCounter {
        private long windowStart = System.currentTimeMillis();
        private int count;
    }
}
