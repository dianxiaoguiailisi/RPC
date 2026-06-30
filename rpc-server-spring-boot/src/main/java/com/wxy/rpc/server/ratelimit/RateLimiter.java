package com.wxy.rpc.server.ratelimit;

/**
 * 服务端限流器接口。
 *
 * 限流器运行在 provider 侧，用于在真正反射调用业务方法之前判断当前请求是否允许执行。
 */
public interface RateLimiter {

    /**
     * 尝试获取一次请求执行许可。
     *
     * @param resource 限流资源名称，当前使用 serviceName#method
     * @return true 表示允许执行，false 表示请求被限流
     */
    boolean tryAcquire(String resource);
}
