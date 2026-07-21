package com.wxy.rpc.client.proxy;

import com.wxy.rpc.client.common.RequestMetadata;
import com.wxy.rpc.client.config.RpcClientProperties;
import com.wxy.rpc.client.transport.RpcClient;
import com.wxy.rpc.core.common.RpcRequest;
import com.wxy.rpc.core.common.RpcResponse;
import com.wxy.rpc.core.common.ServiceInfo;
import com.wxy.rpc.core.discovery.ServiceDiscovery;
import com.wxy.rpc.core.exception.RpcException;
import com.wxy.rpc.core.loadbalance.LoadBalance;
import com.wxy.rpc.core.metrics.RpcMetricNames;
import com.wxy.rpc.core.metrics.RpcMetricsCollector;
import com.wxy.rpc.core.metrics.RpcMetricsContext;
import com.wxy.rpc.core.protocol.MessageHeader;
import com.wxy.rpc.core.protocol.RpcMessage;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 远程方法调用工具类
 */
@Slf4j
public class RemoteMethodCall {

    /**
     * 发起 rpc 远程方法调用的公共方法
     *
     * @param discovery   服务发现中心
     * @param rpcClient   rpc 客户端
     * @param serviceName 服务名称
     * @param properties  配置属性
     * @param loadBalance 当前代理对象使用的负载均衡策略
     * @param method      调用的具体方法
     * @param args        方法参数
     * @return 返回方法调用结果
     */
    public static Object remoteCall(ServiceDiscovery discovery, RpcClient rpcClient, String serviceName,
                                    RpcClientProperties properties, LoadBalance loadBalance, Method method,
                                    Object[] args) {
        return remoteCall(discovery, rpcClient, serviceName, properties, loadBalance, method, args,
                properties.getConsistentHashArguments());
    }

    public static Object remoteCall(ServiceDiscovery discovery, RpcClient rpcClient, String serviceName,
                                    RpcClientProperties properties, LoadBalance loadBalance, Method method,
                                    Object[] args, int[] hashArguments) {
        try {
            return remoteCallAsync(discovery, rpcClient, serviceName, properties, loadBalance, method, args,
                    hashArguments).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RpcException("Remote procedure call was interrupted.", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            if (cause instanceof RpcException) {
                throw (RpcException) cause;
            }
            throw new RpcException(cause);
        }
    }

    /**
     * 判断当前方法是否是异步 RPC 调用。
     *
     * @param method 调用方法
     * @return true 表示方法返回 CompletableFuture
     */
    public static boolean isAsyncReturn(Method method) {
        return CompletableFuture.class.isAssignableFrom(method.getReturnType());
    }

    /**
     * 发起异步 RPC 远程方法调用。
     *
     * @param discovery   服务发现中心
     * @param rpcClient   rpc 客户端
     * @param serviceName 服务名称
     * @param properties  配置属性
     * @param loadBalance 当前代理对象使用的负载均衡策略
     * @param method      调用的具体方法
     * @param args        方法参数
     * @return 返回方法调用结果 Future
     */
    public static CompletableFuture<Object> remoteCallAsync(ServiceDiscovery discovery, RpcClient rpcClient,
                                                            String serviceName, RpcClientProperties properties,
                                                            LoadBalance loadBalance, Method method, Object[] args) {
        return remoteCallAsync(discovery, rpcClient, serviceName, properties, loadBalance, method, args,
                properties.getConsistentHashArguments());
    }

    public static CompletableFuture<Object> remoteCallAsync(ServiceDiscovery discovery, RpcClient rpcClient,
                                                            String serviceName, RpcClientProperties properties,
                                                            LoadBalance loadBalance, Method method, Object[] args,
                                                            int[] hashArguments) {
        long proxyStart = RpcMetricsCollector.now();
        // 构建请求体
        RpcRequest request = new RpcRequest();
        request.setServiceName(serviceName);
        request.setMethod(method.getName());
        request.setParameterTypes(method.getParameterTypes());
        request.setParameterValues(args);
        request.setHashArguments(hashArguments);
        RpcMetricsCollector.recordSince("client", serviceName, method.getName(), RpcMetricNames.CLIENT_PROXY_COST,
                proxyStart);

        int retries = properties.getRetries() == null ? 0 : Math.max(properties.getRetries(), 0);
        int maxAttempts = retries + 1;
        List<ServiceInfo> triedServices = new ArrayList<>(maxAttempts);
        AtomicReference<Exception> lastException = new AtomicReference<>();
        return remoteCallAsyncAttempt(discovery, rpcClient, request, serviceName, properties, loadBalance,
                triedServices, lastException, 1, maxAttempts);
    }

    private static CompletableFuture<Object> remoteCallAsyncAttempt(ServiceDiscovery discovery, RpcClient rpcClient,
                                                                    RpcRequest request, String serviceName,
                                                                    RpcClientProperties properties,
                                                                    LoadBalance loadBalance,
                                                                    List<ServiceInfo> triedServices,
                                                                    AtomicReference<Exception> lastException,
                                                                    int attempt, int maxAttempts) {
        ServiceInfo serviceInfo = selectServiceInfo(discovery, request, triedServices, loadBalance);
        if (serviceInfo == null) {
            CompletableFuture<Object> failedFuture = new CompletableFuture<>();
            failedFuture.completeExceptionally(new RpcException(String.format(
                    "Remote procedure call failed after %d attempt(s), serviceName: %s.",
                    triedServices.size(), serviceName), lastException.get()));
            return failedFuture;
        }
        triedServices.add(serviceInfo);
        long startTime = System.currentTimeMillis();
        ProviderHealthManager.recordCallStart(serviceInfo);
        return doRemoteCallAsync(rpcClient, properties, request, serviceInfo).handle((result, throwable) -> {
            long elapsed = System.currentTimeMillis() - startTime;
            ProviderHealthManager.recordCallEnd(serviceInfo, elapsed);
            if (throwable == null) {
                recordSlowOrFastCall(serviceInfo, properties, elapsed);
                ProviderHealthManager.recordSuccess(serviceInfo);
                return CompletableFuture.completedFuture(result);
            }
            Exception exception = toException(throwable);
            lastException.set(exception);
            ProviderHealthManager.recordFailure(serviceInfo, properties.getFailureThreshold(),
                    properties.getCircuitOpenMillis());
            log.warn("Remote procedure call failed, serviceName: {}, provider: {}:{}, attempt: {}/{}.",
                    serviceName, serviceInfo.getAddress(), serviceInfo.getPort(), attempt, maxAttempts, exception);
            if (attempt >= maxAttempts || !shouldRetry(exception, properties)) {
                CompletableFuture<Object> failedFuture = new CompletableFuture<>();
                failedFuture.completeExceptionally(new RpcException(String.format(
                        "Remote procedure call failed after %d attempt(s), serviceName: %s.",
                        triedServices.size(), serviceName), exception));
                return failedFuture;
            }
            return remoteCallAsyncAttempt(discovery, rpcClient, request, serviceName, properties, loadBalance,
                    triedServices, lastException, attempt + 1, maxAttempts);
        }).thenCompose(resultFuture -> resultFuture);
    }

    /**
     * 选择本次调用要访问的服务节点。
     *
     * 每次重试都会重新执行服务发现，并尽量避开已经失败过的 provider。
     * 如果负载均衡选中的节点已经失败过，则从当前服务列表中选择一个还没尝试过的节点。
     *
     * @param discovery 服务发现组件
     * @param request RPC 请求
     * @param triedServices 已经尝试过的服务节点
     * @param loadBalance 当前代理对象使用的负载均衡策略
     * @return 本次要调用的服务节点
     */
    private static ServiceInfo selectServiceInfo(ServiceDiscovery discovery, RpcRequest request,
                                                 List<ServiceInfo> triedServices, LoadBalance loadBalance) {
        try {
            long discoveryStart = RpcMetricsCollector.now();
            List<ServiceInfo> services = discovery.getServices(request.getServiceName());
            RpcMetricsCollector.recordSince("client", request.getServiceName(), request.getMethod(),
                    RpcMetricNames.CLIENT_DISCOVERY_COST, discoveryStart);
            if (services != null) {
                List<ServiceInfo> availableServices = new ArrayList<>(services.size());
                for (ServiceInfo serviceInfo : services) {
                    if (!triedServices.contains(serviceInfo) && ProviderHealthManager.isAvailable(serviceInfo)) {
                        availableServices.add(serviceInfo);
                    }
                }
                long loadBalanceStart = RpcMetricsCollector.now();
                ServiceInfo selected = loadBalance.select(availableServices, request);
                RpcMetricsCollector.recordSince("client", request.getServiceName(), request.getMethod(),
                        RpcMetricNames.CLIENT_LOAD_BALANCE_COST, loadBalanceStart);
                return selected;
            }
        } catch (Exception e) {
            throw new RpcException(String.format("Remote service discovery did not find service %s.",
                    request.getServiceName()), e);
        }
        return null;
    }

    /**
     * 向指定 provider 发起一次 RPC 调用。
     *
     * 每次调用都会重新构建 MessageHeader，从而生成新的 sequenceId。
     * 这样重试请求不会和前一次超时请求的响应混淆。
     *
     * @param rpcClient RPC 客户端
     * @param properties 客户端配置
     * @param request RPC 请求
     * @param serviceInfo 本次调用的服务节点
     * @return 远程方法返回值
     */
    private static Object doRemoteCall(RpcClient rpcClient, RpcClientProperties properties,
                                       RpcRequest request, ServiceInfo serviceInfo) {
        try {
            return doRemoteCallAsync(rpcClient, properties, request, serviceInfo).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RpcException("Remote procedure call was interrupted.", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            if (cause instanceof RpcException) {
                throw (RpcException) cause;
            }
            throw new RpcException(cause);
        }
    }

    private static CompletableFuture<Object> doRemoteCallAsync(RpcClient rpcClient, RpcClientProperties properties,
                                                               RpcRequest request, ServiceInfo serviceInfo) {
        long totalStart = RpcMetricsCollector.now();
        // 构建请求头
        MessageHeader header = MessageHeader.build(properties.getSerialization());
        RpcMetricsContext.register(header.getSequenceId(), request.getServiceName(), request.getMethod(), totalStart);
        // 构建通信协议信息
        RpcMessage rpcMessage = new RpcMessage();
        rpcMessage.setHeader(header);
        rpcMessage.setBody(request);

        // 构建请求元数据
        RequestMetadata metadata = RequestMetadata.builder()
                .rpcMessage(rpcMessage)
                .serverAddr(serviceInfo.getAddress())
                .port(serviceInfo.getPort())
                .timeout(properties.getTimeout()).build();

        long waitStart = RpcMetricsCollector.now();
        CompletableFuture<RpcMessage> responseFuture = rpcClient.sendRpcRequestAsync(metadata);
        responseFuture.whenComplete((responseRpcMessage, throwable) ->
                RpcMetricsCollector.recordSince("client", request.getServiceName(), request.getMethod(),
                        RpcMetricNames.CLIENT_WAIT_RESPONSE_COST, waitStart));
        return responseFuture.thenApply(responseRpcMessage -> {
            if (responseRpcMessage == null) {
                throw new RpcException("Remote procedure call timeout.");
            }

            // 获取响应结果
            RpcResponse response = (RpcResponse) responseRpcMessage.getBody();

            // 如果 远程调用 发生错误
            if (response.getExceptionValue() != null) {
                throw new RpcException(response.getExceptionValue());
            }
            // 返回响应结果
            return response.getReturnValue();
        });
    }

    /**
     * 根据调用耗时记录成功或慢调用。
     *
     * 慢调用并不代表本次请求失败，所以本次请求仍然正常返回。
     * 但如果同一 provider 连续慢调用达到阈值，后续请求会短时间避开该节点。
     *
     * @param serviceInfo 服务节点
     * @param properties 客户端配置
     * @param elapsedMillis 调用耗时，单位毫秒
     */
    private static void recordSlowOrFastCall(ServiceInfo serviceInfo, RpcClientProperties properties,
                                             long elapsedMillis) {
        long slowThreshold = properties.getSlowCallThresholdMillis() == null
                ? Long.MAX_VALUE : Math.max(properties.getSlowCallThresholdMillis(), 1L);
        if (elapsedMillis >= slowThreshold) {
            ProviderHealthManager.recordSlowCall(serviceInfo, properties.getSlowCallThresholdCount(),
                    properties.getCircuitOpenMillis());
        } else {
            ProviderHealthManager.recordFastCall(serviceInfo);
        }
    }

    /**
     * 判断异常是否属于可重试白名单。
     *
     * 超时、连接失败、网络异常通常可以重试。
     * 服务端返回的业务异常默认不重试，避免非幂等业务被重复执行。
     *
     * @param exception 调用异常
     * @param properties 客户端配置
     * @return true 表示允许继续重试
     */
    private static boolean shouldRetry(Exception exception, RpcClientProperties properties) {
        Throwable cause = unwrap(exception);
        if (cause instanceof TimeoutException) {
            return Boolean.TRUE.equals(properties.getRetryOnTimeout());
        }
        if (cause instanceof ConnectException || cause instanceof UnknownHostException || cause instanceof SocketException) {
            return Boolean.TRUE.equals(properties.getRetryOnConnectFailure());
        }
        if (cause instanceof RpcException) {
            return Boolean.TRUE.equals(properties.getRetryOnBusinessException());
        }
        return false;
    }

    private static Throwable unwrap(Throwable throwable) {
        Throwable current = throwable;
        if (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current;
    }

    private static Exception toException(Throwable throwable) {
        Throwable cause = unwrap(throwable);
        if (cause instanceof Exception) {
            return (Exception) cause;
        }
        return new RpcException(cause);
    }
}
