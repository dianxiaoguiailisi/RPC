package com.wxy.rpc.server.handler;

import com.wxy.rpc.core.common.RpcRequest;
import com.wxy.rpc.core.exception.RpcException;
import com.wxy.rpc.server.ratelimit.RateLimiterFactory;
import com.wxy.rpc.server.store.LocalServiceCache;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Rpc 请求调用处理器
 *
 * @author Wuxy
 * @version 1.0
 * @ClassName RpcRequestHandler
 * @Date 2023/1/6 19:42
 */
@Slf4j
public class RpcRequestHandler {

    /**
     * 目标方法缓存。
     *
     * key 由 serviceName、服务实现类、方法名和参数类型共同组成，用来区分不同服务和重载方法。
     * value 是已经解析好的 Method 对象。
     *
     * 服务端处理高频 RPC 请求时，同一个接口方法会被反复调用。如果每次都通过 getMethod 查找反射元数据，
     * 会产生重复的方法查找开销。这里缓存 Method 后，后续相同方法调用可以直接复用 Method，只保留 invoke 调用。
     */
    private static final Map<String, Method> METHOD_CACHE = new ConcurrentHashMap<>();

    /**
     * 处理 RpcRequest
     *
     * @param request rpc request 对象
     * @return 返回方法调用结果
     * @throws Exception 反射调用方法失败，抛出异常
     */
    public Object handleRpcRequest(RpcRequest request) throws Exception {
        String rateLimitResource = request.getServiceName() + "#" + request.getMethod();
        if (!RateLimiterFactory.getRateLimiter().tryAcquire(rateLimitResource)) {
            log.warn("The service method [{}] request was rejected by rate limiter.", rateLimitResource);
            throw new RpcException(String.format("The service method [%s] request was rejected by rate limiter.",
                    rateLimitResource));
        }
        // 反射调用 RpcRequest 请求指定的方法
        // 获取请求服务实例
        Object service = LocalServiceCache.getService(request.getServiceName());
        if (service == null) {
            log.error("The service [{}] is not exist!", request.getServiceName());
            throw new RpcException(String.format("The service [%s] is not exist!", request.getServiceName()));
        }
        // 获取指定的方法，同一个服务方法只在第一次调用时执行反射查找，后续直接复用缓存的 Method
        Method method = getTargetMethod(service, request);
        // 调用方法并返回结果
        return method.invoke(service, request.getParameterValues());
    }

    /**
     * 根据请求信息获取目标方法。
     *
     * @param service 服务实现对象
     * @param request RPC 请求
     * @return 目标 Method
     * @throws NoSuchMethodException 方法不存在时抛出
     */
    private Method getTargetMethod(Object service, RpcRequest request) throws NoSuchMethodException {
        String methodKey = buildMethodKey(service, request);
        Method cachedMethod = METHOD_CACHE.get(methodKey);
        if (cachedMethod != null) {
            return cachedMethod;
        }

        Method method = service.getClass().getMethod(request.getMethod(), request.getParameterTypes());
        method.setAccessible(true);
        Method oldMethod = METHOD_CACHE.putIfAbsent(methodKey, method);
        return oldMethod == null ? method : oldMethod;
    }

    /**
     * 构建方法缓存 key。
     *
     * serviceName 用来区分不同 RPC 服务，serviceClass 用来避免同名服务实现类替换时误复用，
     * methodName 和 parameterTypes 用来区分普通方法和重载方法。
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
