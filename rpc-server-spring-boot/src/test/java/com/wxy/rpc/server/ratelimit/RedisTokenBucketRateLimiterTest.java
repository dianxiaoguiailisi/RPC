package com.wxy.rpc.server.ratelimit;

import junit.framework.TestCase;
import redis.clients.jedis.Jedis;

/**
 * Redis 分布式令牌桶测试。
 */
public class RedisTokenBucketRateLimiterTest extends TestCase {

    private static final String HOST = "127.0.0.1";

    private static final int PORT = 6379;

    private static final String PREFIX = "wxy-rpc-test:rate-limit:";

    public void testTwoLimitersShareSameRedisBucketWithLocalPrefetch() throws Exception {
        if (!isRedisAvailable()) {
            System.out.println("Redis is not available, skip redis token bucket integration test.");
            return;
        }
        String resource = "demoService#hello";
        clearRedisKey(resource);

        RateLimiter firstProviderLimiter = newLimiter();
        RateLimiter secondProviderLimiter = newLimiter();

        assertTrue(firstProviderLimiter.tryAcquire(resource));
        assertTrue(firstProviderLimiter.tryAcquire(resource));
        assertTrue(firstProviderLimiter.tryAcquire(resource));

        assertTrue(secondProviderLimiter.tryAcquire(resource));
        assertFalse(secondProviderLimiter.tryAcquire(resource));
        assertFalse(firstProviderLimiter.tryAcquire(resource));

        Thread.sleep(1100L);
        assertTrue(secondProviderLimiter.tryAcquire(resource));
    }

    public void testFallbackToLocalTokenBucketWhenRedisUnavailable() {
        RateLimiter fallbackLimiter = new TokenBucketRateLimiter(1, 10_000L, 1);
        RateLimiter limiter = new RedisTokenBucketRateLimiter(1, 10_000L, 1,
                HOST, 1, "", 0, 100, PREFIX, 10, true, false, fallbackLimiter);

        assertTrue(limiter.tryAcquire("fallbackService#hello"));
        assertFalse(limiter.tryAcquire("fallbackService#hello"));
    }

    private RateLimiter newLimiter() {
        return new RedisTokenBucketRateLimiter(4, 1000L, 4,
                HOST, PORT, "", 0, 1000, PREFIX, 3, false, false, new NoopRateLimiter());
    }

    private boolean isRedisAvailable() {
        try (Jedis jedis = new Jedis(HOST, PORT)) {
            return "PONG".equals(jedis.ping());
        } catch (Exception e) {
            return false;
        }
    }

    private void clearRedisKey(String resource) {
        try (Jedis jedis = new Jedis(HOST, PORT)) {
            jedis.del(PREFIX + resource);
        }
    }
}
