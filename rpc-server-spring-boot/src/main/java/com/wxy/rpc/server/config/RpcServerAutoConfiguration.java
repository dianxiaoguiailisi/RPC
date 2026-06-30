package com.wxy.rpc.server.config;

import com.wxy.rpc.core.registry.ServiceRegistry;
import com.wxy.rpc.core.registry.nacos.NacosServiceRegistry;
import com.wxy.rpc.core.registry.zk.ZookeeperServiceRegistry;
import com.wxy.rpc.server.ratelimit.RateLimiterFactory;
import com.wxy.rpc.server.spring.RpcServerBeanPostProcessor;
import com.wxy.rpc.server.threadpool.BusinessThreadPoolManager;
import com.wxy.rpc.server.transport.RpcServer;
import com.wxy.rpc.server.transport.http.HttpRpcServer;
import com.wxy.rpc.server.transport.netty.NettyRpcServer;
import com.wxy.rpc.server.transport.socket.SocketRpcServer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * RpcServer 端的自动配置类
 *
 * @author Wuxy
 * @version 1.0
 * @ClassName RpcServerAutoConfiguration
 * @Date 2023/1/8 12:34
 */
@Configuration
@EnableConfigurationProperties(RpcServerProperties.class)
public class RpcServerAutoConfiguration {

    @Autowired
    RpcServerProperties properties;

    /**
     * 初始化服务端限流器。
     *
     * RpcRequestHandler 通过 SingletonFactory 创建，不直接受 Spring 管理，
     * 所以这里使用静态工厂把限流配置传递给请求处理链路。
     */
    @Bean
    public Object rateLimiterInitializer() {
        RateLimiterFactory.init(properties);
        return new Object();
    }

    /**
     * 初始化服务端业务线程池。
     *
     * NettyRpcRequestHandler 不是由 Spring 直接创建，所以这里通过静态管理器保存线程池实例。
     */
    @Bean
    public Object businessThreadPoolInitializer() {
        BusinessThreadPoolManager.init(properties);
        return new Object();
    }

    @Bean
    public DisposableBean businessThreadPoolDisposableBean() {
        return BusinessThreadPoolManager::shutdown;
    }

    /**
     * 创建 ServiceRegistry 实例 bean，当没有配置时默认使用 zookeeper 作为配置中心
     */
    @Bean(name = "serviceRegistry")
    @Primary
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "rpc.server", name = "registry", havingValue = "zookeeper", matchIfMissing = true)
    public ServiceRegistry zookeeperServiceRegistry() {
        return new ZookeeperServiceRegistry(properties.getRegistryAddr());
    }

    @Bean(name = "serviceRegistry")
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "rpc.server", name = "registry", havingValue = "nacos")
    public ServiceRegistry nacosServiceRegistry() {
        return new NacosServiceRegistry(properties.getRegistryAddr());
    }

    // 当没有配置通信协议属性时，默认使用 netty 作为通讯协议
    @Bean(name = "rpcServer")
    @Primary
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "rpc.server", name = "transport", havingValue = "netty", matchIfMissing = true)
    public RpcServer nettyRpcServer() {
        return new NettyRpcServer();
    }

    @Bean(name = "rpcServer")
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "rpc.server", name = "transport", havingValue = "http")
    @ConditionalOnClass(name = {"org.apache.catalina.startup.Tomcat"})
    public RpcServer httpRpcServer() {
        return new HttpRpcServer();
    }

    @Bean(name = "rpcServer")
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "rpc.server", name = "transport", havingValue = "socket")
    public RpcServer socketRpcServer() {
        return new SocketRpcServer();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean({ServiceRegistry.class, RpcServer.class})
    public RpcServerBeanPostProcessor rpcServerBeanPostProcessor(@Autowired ServiceRegistry serviceRegistry,
                                                                 @Autowired RpcServer rpcServer,
                                                                 @Autowired RpcServerProperties properties) {

        return new RpcServerBeanPostProcessor(serviceRegistry, rpcServer, properties);
    }

}
