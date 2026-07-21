package com.wxy.rpc.server.handler;

import com.wxy.rpc.core.common.RpcRequest;
import com.wxy.rpc.core.exception.RpcException;
import com.wxy.rpc.core.metrics.RpcMetricNames;
import com.wxy.rpc.core.metrics.RpcMetricsCollector;
import com.wxy.rpc.server.ratelimit.RateLimiterFactory;
import com.wxy.rpc.server.store.LocalServiceCache;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * RPC 本地请求调用处理器。
 *
 * <p>当 Netty、HTTP 或 Socket 传输层完成协议解码后，会将 {@link RpcRequest}
 * 交给当前处理器。当前处理器不负责网络收发，只负责服务端本地调用：</p>
 * <ol>
 *     <li>根据服务名和方法名执行服务端限流。</li>
 *     <li>通过 {@link LocalServiceCache} 获取真实的服务实例。</li>
 *     <li>根据方法名和参数类型查找并缓存目标 {@link Method}。</li>
 *     <li>反射调用目标方法，返回业务结果并记录调用指标。</li>
 * </ol>
 */
@Slf4j
public class RpcRequestHandler {

    /**
     * 目标方法缓存。
     *
     * <p>Key 由 serviceName、服务实现类、方法名和参数类型共同组成，
     * 用来区分不同服务、不同实现以及重载方法；Value 是已经解析好的 Method 对象。</p>
     *
     * <p>同一个服务方法可能被高频调用。缓存 Method 后，只有第一次调用需要执行
     * {@link Class#getMethod(String, Class[])} 查找，后续调用可以直接复用反射元数据。</p>
     *
     * <p>当前处理器可能被多个业务线程并发调用，因此使用 {@link ConcurrentHashMap}
     * 保证方法查询和缓存写入的线程安全。缓存条目数取决于不同服务方法数量，
     * 而不是 RPC 请求数量。</p>
     */
    private static final Map<String, Method> METHOD_CACHE = new ConcurrentHashMap<>();

    /**
     * 处理 RPC 请求并调用本地服务方法。
     *
     * <p>执行顺序为：限流检查 → 查找本地服务 → 获取目标方法 → 反射调用。
     * 当服务不存在、请求被限流或反射调用失败时，异常会向上抛出，
     * 由上层传输处理器封装为 RPC 失败响应。</p>
     *
     * @param request 已解码的 RPC 请求
     * @return 本地服务方法的调用结果
     * @throws Exception 限流、服务查找或反射调用失败时抛出
     */
    public Object handleRpcRequest(RpcRequest request) throws Exception {
        // 将服务名和方法名拼接成限流资源名称。
        String rateLimitResource = request.getServiceName() + "#" + request.getMethod();
        long rateLimitStart = RpcMetricsCollector.now();//记录限流开始时间
        if (!RateLimiterFactory.getRateLimiter().tryAcquire(rateLimitResource)) {//尝试获取限流许可
            RpcMetricsCollector.recordSince("server", request.getServiceName(), request.getMethod(),RpcMetricNames.SERVER_RATE_LIMIT_COST, rateLimitStart);
            log.warn("The service method [{}] request was rejected by rate limiter.", rateLimitResource);
            throw new RpcException(String.format("The service method [%s] request was rejected by rate limiter.",rateLimitResource));
        }
        RpcMetricsCollector.recordSince("server", request.getServiceName(), request.getMethod(),RpcMetricNames.SERVER_RATE_LIMIT_COST, rateLimitStart);
        long serviceLookupStart = RpcMetricsCollector.now();
        //查找本地服务 Bean
        Object service = LocalServiceCache.getService(request.getServiceName());//根据服务名称获取真实服务对象
        RpcMetricsCollector.recordSince("server", request.getServiceName(), request.getMethod(),RpcMetricNames.SERVER_LOCAL_SERVICE_LOOKUP_COST, serviceLookupStart);//记录本地服务查找耗时
        //判断服务是否存在
        if (service == null) {
            log.error("The service [{}] is not exist!", request.getServiceName());
            throw new RpcException(String.format("The service [%s] is not exist!", request.getServiceName()));
        }
        //查找目标 Method
        long methodLookupStart = RpcMetricsCollector.now();
        Method method = getTargetMethod(service, request);
        RpcMetricsCollector.recordSince("server", request.getServiceName(), request.getMethod(),RpcMetricNames.SERVER_METHOD_LOOKUP_COST, methodLookupStart);
        // 使用请求中的参数值反射调用真实服务方法。
        long invokeStart = RpcMetricsCollector.now();
        try {
            return method.invoke(service, request.getParameterValues());
        } finally {
            long invokeCost = RpcMetricsCollector.now() - invokeStart;
            RpcMetricsCollector.record("server", request.getServiceName(), request.getMethod(),
                    RpcMetricNames.SERVER_REFLECT_INVOKE_COST, invokeCost);
            RpcMetricsCollector.record("server", request.getServiceName(), request.getMethod(),
                    RpcMetricNames.SERVER_BIZ_COST, invokeCost);
        }
    }

    /**
     * 根据请求信息获取目标方法。
     *
     * <p>首先查询方法缓存；缓存未命中时，根据方法名和参数类型从服务实现类中查找 Method。
     * 使用 {@link ConcurrentHashMap#putIfAbsent(Object, Object)} 处理多个线程同时首次调用同一方法的竞争，
     * 保证最终复用缓存中的 Method 对象。</p>
     *
     * @param service 服务实现对象
     * @param request RPC 请求
     * @return 目标 Method
     * @throws NoSuchMethodException 方法不存在时抛出
     */
    private Method getTargetMethod(Object service, RpcRequest request) throws NoSuchMethodException {
        String methodKey = buildMethodKey(service, request);//根据服务对象和 RPC 请求生成方法缓存键
        Method cachedMethod = METHOD_CACHE.get(methodKey);
        if (cachedMethod != null) {
            return cachedMethod;
        }
        // 仅在缓存未命中时执行反射方法查找。（通过反射查找目标方法）
        Method method = service.getClass().getMethod(request.getMethod(), request.getParameterTypes());
        method.setAccessible(true);
        Method oldMethod = METHOD_CACHE.putIfAbsent(methodKey, method);
        return oldMethod == null ? method : oldMethod;
    }

    /**
     * 构建方法缓存 key。
     * <p>serviceName 用来区分不同 RPC 服务，serviceClass 用来区分不同服务实现，
     * methodName 和 parameterTypes 用来区分普通方法与重载方法。</p>
     *
     * <p>示例：zx110501$
     * {@code com.example.UserService-1.0#com.example.UserServiceImpl#getUser(java.lang.Long,)}</p>
     *
     * @param service 本地服务实例
     * @param request RPC 请求
     * @return 可唯一区分目标方法的缓存 Key
     */
    private String buildMethodKey(Object service, RpcRequest request) {
        StringBuilder builder = new StringBuilder(128);
        builder.append(request.getServiceName())
                .append('#')
                .append(service.getClass().getName())
                .append('#')
                .append(request.getMethod())
                .append('(');
        Class<?>[] parameterTypes = request.getParameterTypes();
        if (parameterTypes != null) {
            for (Class<?> parameterType : parameterTypes) {
                builder.append(parameterType == null ? "null" : parameterType.getName()).append(',');
            }
        }
        builder.append(')');
        return builder.toString();
    }

}
