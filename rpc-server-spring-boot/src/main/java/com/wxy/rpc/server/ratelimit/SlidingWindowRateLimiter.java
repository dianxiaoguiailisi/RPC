package com.wxy.rpc.server.ratelimit;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 滑动窗口限流器。
 *
 * 保存窗口内每个请求的时间戳，每次请求到来时移除窗口外的旧请求，
 * 再判断窗口内请求数是否超过阈值。
 */
public class SlidingWindowRateLimiter implements RateLimiter {

    private final int permits;

    private final long windowMillis;

    private final ConcurrentMap<String, Deque<Long>> requestTimes = new ConcurrentHashMap<>();

    public SlidingWindowRateLimiter(int permits, long windowMillis) {
        this.permits = Math.max(permits, 1);
        this.windowMillis = Math.max(windowMillis, 1L);
    }

    @Override
    public boolean tryAcquire(String resource) {
        Deque<Long> timestamps = requestTimes.computeIfAbsent(resource, key -> new ArrayDeque<>());
        synchronized (timestamps) {
            long now = System.currentTimeMillis();
            long windowStart = now - windowMillis;
            while (!timestamps.isEmpty() && timestamps.peekFirst() <= windowStart) {
                timestamps.pollFirst();
            }
            if (timestamps.size() >= permits) {
                return false;
            }
            timestamps.offerLast(now);
            return true;
        }
    }
}
