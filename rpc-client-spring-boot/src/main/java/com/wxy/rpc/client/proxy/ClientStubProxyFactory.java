package com.wxy.rpc.client.proxy;

import com.wxy.rpc.client.config.RpcClientProperties;
import com.wxy.rpc.client.transport.RpcClient;
import com.wxy.rpc.core.discovery.ServiceDiscovery;
import com.wxy.rpc.core.extension.ExtensionLoader;
import com.wxy.rpc.core.loadbalance.LoadBalance;
import com.wxy.rpc.core.util.ServiceUtil;
import net.sf.cglib.proxy.Enhancer;

import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 客户端代理工厂类，返回服务代理类
 *
 * @author Wuxy
 * @version 1.0
 * @ClassName ClientStubProxyFactory
 * @Date 2023/1/7 14:54
 */
public class ClientStubProxyFactory {

    /**
     * 服务发现中心实现类
     */
    private final ServiceDiscovery discovery;

    /**
     * RpcClient 传输实现类
     */
    private final RpcClient rpcClient;

    /**
     * 客户端配置属性
     */
    private final RpcClientProperties properties;

    public ClientStubProxyFactory(ServiceDiscovery discovery, RpcClient rpcClient, RpcClientProperties properties) {
        this.discovery = discovery;
        this.rpcClient = rpcClient;
        this.properties = properties;
    }

    /**
     * 代理对象缓存
     */
    private static final Map<String, Object> proxyMap = new ConcurrentHashMap<>();

    /**
     * 获取代理对象
     *
     * @param clazz   服务接口类型
     * @param version 版本号
     * @param <T>     代理对象的参数类型
     * @return 对应版本的代理对象
     */
    @SuppressWarnings("unchecked")
    public <T> T getProxy(Class<T> clazz, String version) {
        return getProxy(clazz, version, properties.getLoadbalance(), properties.getConsistentHashArguments());
    }

    /**
     * 获取代理对象。
     *
     * @param clazz       服务接口类型
     * @param version     版本号
     * @param loadbalance 当前引用指定的负载均衡策略，为空时使用全局配置
     * @param <T>         代理对象的参数类型
     * @return 对应版本和负载均衡策略的代理对象
     */
    @SuppressWarnings("unchecked")
    public <T> T getProxy(Class<T> clazz, String version, String loadbalance) {
        return getProxy(clazz, version, loadbalance, properties.getConsistentHashArguments());
    }

    /**
     * 获取代理对象。
     *
     * @param clazz 服务接口类型
     * @param version 版本号
     * @param loadbalance 当前引用指定的负载均衡策略，为空时使用全局配置
     * @param hashArguments 一致性哈希参与计算的参数下标
     * @param <T> 代理对象的参数类型
     * @return 对应版本和引用配置的代理对象
     */
    @SuppressWarnings("unchecked")
    public <T> T getProxy(Class<T> clazz, String version, String loadbalance, int[] hashArguments) {
        String effectiveLoadbalance = resolveLoadBalanceName(loadbalance);
        int[] effectiveHashArguments = resolveHashArguments(hashArguments);
        LoadBalance loadBalance = getLoadBalance(effectiveLoadbalance);
        String serviceName = ServiceUtil.serviceKey(clazz.getName(), version);
        String proxyCacheKey = serviceName + ":" + effectiveLoadbalance + ":" + Arrays.toString(effectiveHashArguments);
        return (T) proxyMap.computeIfAbsent(proxyCacheKey, key -> {
            // 如果目标类是一个接口或者 是 java.lang.reflect.Proxy 的子类 则默认使用 JDK 动态代理
            if (clazz.isInterface() || Proxy.isProxyClass(clazz)) {

                return Proxy.newProxyInstance(clazz.getClassLoader(),
                        new Class[]{clazz}, // 注意，这里的接口是 clazz 本身（即，要代理的实现类所实现的接口）
                        new ClientStubInvocationHandler(discovery, rpcClient, properties, loadBalance, serviceName,
                                effectiveHashArguments));
            } else { // 使用 CGLIB 动态代理
                // 创建动态代理增加类
                Enhancer enhancer = new Enhancer();
                // 设置类加载器
                enhancer.setClassLoader(clazz.getClassLoader());
                // 设置被代理类
                enhancer.setSuperclass(clazz);
                // 设置方法拦截器
                enhancer.setCallback(new ClientStubMethodInterceptor(discovery, rpcClient, properties, loadBalance,
                        serviceName, effectiveHashArguments));
                // 创建代理类
                return enhancer.create();
            }
        });
    }

    /**
     * 解析当前引用实际使用的负载均衡策略。
     *
     * @param loadbalance 注解上指定的策略
     * @return 实际策略名称
     */
    public String resolveLoadBalanceName(String loadbalance) {
        if (loadbalance != null && !loadbalance.trim().isEmpty()) {
            return loadbalance.trim();
        }
        String globalLoadBalance = properties.getLoadbalance();
        if (globalLoadBalance == null || globalLoadBalance.trim().isEmpty()) {
            return "random";
        }
        return globalLoadBalance.trim();
    }

    public int[] resolveHashArguments(int[] hashArguments) {
        if (hashArguments != null && hashArguments.length > 0) {
            return Arrays.copyOf(hashArguments, hashArguments.length);
        }
        int[] globalHashArguments = properties.getConsistentHashArguments();
        if (globalHashArguments == null || globalHashArguments.length == 0) {
            return new int[]{0};
        }
        return Arrays.copyOf(globalHashArguments, globalHashArguments.length);
    }

    private LoadBalance getLoadBalance(String loadbalance) {
        return ExtensionLoader.getExtensionLoader(LoadBalance.class).getExtension(loadbalance);
    }

}
