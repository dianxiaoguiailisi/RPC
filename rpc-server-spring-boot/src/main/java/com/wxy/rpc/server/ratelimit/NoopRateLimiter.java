package com.wxy.rpc.server.ratelimit;

/**
 * 空限流器。
 *
 * 当服务端没有开启限流时使用，所有请求都会放行。
 */
public class NoopRateLimiter implements RateLimiter {

    @Override
    public boolean tryAcquire(String resource) {
        return true;
    }
}
