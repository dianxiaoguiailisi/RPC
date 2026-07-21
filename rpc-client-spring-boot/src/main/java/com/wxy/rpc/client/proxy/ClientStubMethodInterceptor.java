package com.wxy.rpc.client.proxy;

import com.wxy.rpc.client.config.RpcClientProperties;
import com.wxy.rpc.client.transport.RpcClient;
import com.wxy.rpc.core.discovery.ServiceDiscovery;
import com.wxy.rpc.core.loadbalance.LoadBalance;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import java.lang.reflect.Method;

/**
 * 基于 Cglib 动态代理的客户端方法调用处理器类
 */
public class ClientStubMethodInterceptor implements MethodInterceptor {

    /**
     * 服务发现中心
     */
    private final ServiceDiscovery serviceDiscovery;

    /**
     * Rpc客户端
     */
    private final RpcClient rpcClient;

    /**
     * Rpc 客户端配置属性
     */
    private final RpcClientProperties properties;

    /**
     * 当前代理对象使用的负载均衡策略
     */
    private final LoadBalance loadBalance;

    /**
     * 服务名称：接口-版本
     */
    private final String serviceName;

    /**
     * 一致性哈希参与计算的参数下标。
     */
    private final int[] hashArguments;

    public ClientStubMethodInterceptor(ServiceDiscovery serviceDiscovery, RpcClient rpcClient,
                                       RpcClientProperties properties, LoadBalance loadBalance, String serviceName,
                                       int[] hashArguments) {
        this.serviceDiscovery = serviceDiscovery;
        this.rpcClient = rpcClient;
        this.properties = properties;
        this.loadBalance = loadBalance;
        this.serviceName = serviceName;
        this.hashArguments = hashArguments;
    }

    @Override
    public Object intercept(Object o, Method method, Object[] args, MethodProxy methodProxy) throws Throwable {
        if (RemoteMethodCall.isAsyncReturn(method)) {
            return RemoteMethodCall.remoteCallAsync(serviceDiscovery, rpcClient, serviceName, properties, loadBalance,
                    method, args, hashArguments);
        }
        // 执行远程方法调用
        return RemoteMethodCall.remoteCall(serviceDiscovery, rpcClient, serviceName, properties, loadBalance, method,
                args, hashArguments);
    }
}
