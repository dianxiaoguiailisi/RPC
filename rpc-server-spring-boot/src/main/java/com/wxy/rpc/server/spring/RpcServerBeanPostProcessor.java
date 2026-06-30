package com.wxy.rpc.server.spring;

import com.wxy.rpc.core.common.ServiceInfo;
import com.wxy.rpc.core.registry.ServiceRegistry;
import com.wxy.rpc.core.util.ServiceUtil;
import com.wxy.rpc.server.annotation.RpcService;
import com.wxy.rpc.server.config.RpcServerProperties;
import com.wxy.rpc.server.store.LocalServiceCache;
import com.wxy.rpc.server.transport.RpcServer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.CommandLineRunner;

/**
 * RPC 服务端 Bean 后置处理器。
 *
 * 这个类接在 RpcBeanDefinitionRegistrar 和 RpcClassPathBeanDefinitionScanner 之后工作。
 * 前面两个类负责把 @RpcService 类扫描并注册进 Spring 容器；
 * 当前类负责在这些 Bean 初始化完成后，把它们真正暴露成 RPC 服务。
 *
 * 主要职责：
 * 1. 识别带 @RpcService 注解的服务实现 Bean。
 * 2. 解析服务接口名、版本号，生成服务唯一名称 serviceName。
 * 3. 将服务地址等元信息注册到注册中心，例如 Zookeeper/Nacos。
 * 4. 将服务实现对象放入本地缓存，供后续 RPC 请求反射调用。
 * 5. 在 Spring Boot 启动完成后启动 RPC Server，例如 NettyRpcServer。
 */
@Slf4j
public class RpcServerBeanPostProcessor implements BeanPostProcessor, CommandLineRunner {

    /**
     * 服务注册中心接口。
     *
     * 根据配置不同，实际实现可能是 ZookeeperServiceRegistry 或 NacosServiceRegistry。
     */
    private final ServiceRegistry serviceRegistry;

    /**
     * RPC 服务端接口。
     *
     * 根据配置不同，实际实现可能是 NettyRpcServer、SocketRpcServer 或 HttpRpcServer。
     */
    private final RpcServer rpcServer;

    /**
     * RPC 服务端配置，对应配置文件中的 rpc.server.*。
     *
     * 例如应用名、服务端口、注册地址、注册中心类型、通信协议等。
     */
    private final RpcServerProperties properties;

    /**
     * 通过 Spring 自动配置注入注册中心、RPC 服务端和配置属性。
     *
     * @param serviceRegistry 服务注册中心
     * @param rpcServer RPC 服务端
     * @param properties RPC 服务端配置属性
     */
    public RpcServerBeanPostProcessor(ServiceRegistry serviceRegistry, RpcServer rpcServer, RpcServerProperties properties) {
        this.serviceRegistry = serviceRegistry;
        this.rpcServer = rpcServer;
        this.properties = properties;
    }

    /**
     * Bean 初始化完成之后执行。
     *
     * Spring 容器中每个 Bean 初始化完成后，都会经过这个方法。
     * 这里会判断当前 Bean 是否带有 @RpcService 注解。
     *
     * 如果是 RPC 服务 Bean，则执行两类注册：
     * 1. 远程注册：把 ServiceInfo 注册到注册中心，让客户端可以发现这个服务。
     * 2. 本地注册：把 serviceName 和真实 Bean 对象放入 LocalServiceCache，
     *    后续服务端收到 RPC 请求时，可以根据 serviceName 找到实现类并反射调用。
     *
     * @param bean     当前初始化完成的 Bean 对象
     * @param beanName 当前 Bean 在 Spring 容器中的名称
     * @return 返回原始 Bean 对象
     * @throws BeansException Bean 异常
     */
    @SneakyThrows
    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        // 判断当前 bean 是否被 @RpcService 注解标注
        if (bean.getClass().isAnnotationPresent(RpcService.class)) {
            log.info("[{}] is annotated with [{}].", bean.getClass().getName(), RpcService.class.getCanonicalName());
            // 获取到该类的 @RpcService 注解
            RpcService rpcService = bean.getClass().getAnnotation(RpcService.class);
            String interfaceName;
            // 优先使用 interfaceName；如果没有配置，则使用 interfaceClass 的全限定类名。
            if ("".equals(rpcService.interfaceName())) {
                interfaceName = rpcService.interfaceClass().getName();
            } else {
                interfaceName = rpcService.interfaceName();
            }
            String version = rpcService.version();
            // 服务唯一名称，格式为：接口全限定名-版本号。
            // 客户端服务发现和服务端本地缓存都使用这个 key 来匹配服务。
            String serviceName = ServiceUtil.serviceKey(interfaceName, version);
            // 构建 ServiceInfo 对象
            ServiceInfo serviceInfo = ServiceInfo.builder()
                    .appName(properties.getAppName())
                    .serviceName(serviceName)
                    .version(version)
                    .address(properties.getAddress())
                    .port(properties.getPort())
                    .build();
            // 进行远程服务注册
            serviceRegistry.register(serviceInfo);
            // 进行本地服务缓存注册
            LocalServiceCache.addService(serviceName, bean);
        }
        return bean;
    }

    /**
     * Spring Boot 启动完成后执行，用于启动 RPC 服务端。
     *
     * 这里实现的是 CommandLineRunner 接口，所以 Spring Boot 容器准备完成后会调用该方法。
     * RPC Server 启动后会监听配置中的端口，等待客户端发起远程调用。
     *
     * @param args 启动参数
     * @throws Exception 启动异常
     */
    @Override
    public void run(String... args) throws Exception {
        // RPC Server 的 start 方法通常会阻塞当前线程，所以这里新开线程启动。
        new Thread(() -> rpcServer.start(properties.getPort())).start();
        log.info("Rpc server [{}] start, the appName is {}, the port is {}",
                rpcServer, properties.getAppName(), properties.getPort());
        // JVM 关闭时释放注册中心资源。
        // 对 Zookeeper 来说，连接关闭后临时节点会随会话结束而清理，从而实现服务下线。
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                // 当服务关闭之后，将服务从 注册中心 上清除（关闭连接）
                serviceRegistry.destroy();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }));
    }
}
