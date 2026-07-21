package com.wxy.rpc.client.spring;

import com.wxy.rpc.client.proxy.ClientStubProxyFactory;
import org.springframework.beans.factory.FactoryBean;

/**
 * RPC 远程引用 FactoryBean。
 *
 * 这个类借鉴 Dubbo ReferenceBean 的思路，把一次远程服务引用抽象成一个 Spring FactoryBean。
 * 它自身保存接口类型、版本号和代理工厂，真正注入到业务字段里的对象是 getObject 方法返回的代理对象。
 *
 * 这样做的好处是：远程引用不再只是 RpcClientBeanPostProcessor 里临时创建的对象，
 * 而是有了独立的引用模型，后续可以继续承载超时、负载均衡、mock、启动检查等引用级配置。
 *
 * @param <T> 远程服务接口类型
 */
public class RpcReferenceBean<T> implements FactoryBean<T> {

    /**
     * 远程服务接口类型。
     */
    private final Class<T> interfaceClass;

    /**
     * 远程服务版本号。
     */
    private final String version;

    /**
     * 当前远程引用使用的负载均衡策略。
     */
    private final String loadbalance;

    /**
     * 一致性哈希参与计算的参数下标。
     */
    private final int[] hashArguments;

    /**
     * 客户端代理工厂，用于创建真正注入业务字段的远程代理对象。
     */
    private final ClientStubProxyFactory proxyFactory;

    public RpcReferenceBean(Class<T> interfaceClass, String version, String loadbalance, int[] hashArguments,
                            ClientStubProxyFactory proxyFactory) {
        this.interfaceClass = interfaceClass;
        this.version = version;
        this.loadbalance = loadbalance;
        this.hashArguments = hashArguments;
        this.proxyFactory = proxyFactory;
    }

    /**
     * 返回远程服务代理对象。
     *
     * 业务代码调用该代理对象的方法时，会进入 ClientStubInvocationHandler
     * 或 ClientStubMethodInterceptor，并最终发起 RPC 调用。
     *
     * @return 远程服务代理对象
     */
    @Override
    public T getObject() {
        return proxyFactory.getProxy(interfaceClass, version, loadbalance, hashArguments);
    }

    /**
     * 返回 FactoryBean 生产对象的类型。
     *
     * @return 远程服务接口类型
     */
    @Override
    public Class<?> getObjectType() {
        return interfaceClass;
    }

    /**
     * 同一个服务接口和版本只需要一个代理对象。
     *
     * @return true 表示单例
     */
    @Override
    public boolean isSingleton() {
        return true;
    }
}
