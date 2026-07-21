package com.wxy.rpc.core.registry.zk;

import com.wxy.rpc.core.common.ServiceInfo;
import com.wxy.rpc.core.exception.RpcException;
import com.wxy.rpc.core.registry.ServiceRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;

/**
 * 基于 Zookeeper 的服务注册中心实现。
 *
 * 这个类负责服务端的服务注册和下线：
 * 1. 通过 CuratorFramework 维护和 Zookeeper 的连接。
 * 2. 通过 Curator 的 ServiceDiscovery 组件把服务信息注册到 Zookeeper。
 * 3. 服务注册时，将业务侧的 ServiceInfo 包装成 ServiceInstance。
 * 4. 服务关闭时，释放 ServiceDiscovery 和 Zookeeper 客户端连接。
 */
@Slf4j
public class ZookeeperServiceRegistry implements ServiceRegistry {

    /**
     * Zookeeper 会话超时时间。
     *
     * 如果客户端在这个时间内没有和 Zookeeper 恢复心跳，Zookeeper 会认为会话失效，
     * 临时节点也会随会话失效而被清理。
     */
    private static final int SESSION_TIMEOUT = 60 * 1000;

    /**
     * 建立 Zookeeper 连接的超时时间。
     */
    private static final int CONNECT_TIMEOUT = 15 * 1000;

    /**
     * Curator 重试策略的初始等待时间。
     *
     * ExponentialBackoffRetry 会基于这个时间做指数退避重试。
     */
    private static final int BASE_SLEEP_TIME = 3 * 1000;

    /**
     * Curator 连接失败后的最大重试次数。
     */
    private static final int MAX_RETRY = 10;

    /**
     * RPC 服务注册到 Zookeeper 时使用的根路径。
     */
    private static final String BASE_PATH = "/wxy_rpc";

    /**
     * Curator 提供的 Zookeeper 客户端，负责连接管理、重试、会话维护等底层操作。
     */
    private CuratorFramework client;

    /**
     * Curator 的服务发现组件。
     *
     * 它负责把 ServiceInstance 写入 Zookeeper，也负责执行服务注销。
     * ServiceInfo 会作为 payload 被序列化后保存，供客户端服务发现时读取。
     */
    private ServiceDiscovery<ServiceInfo> serviceDiscovery;


    /**
     * 构造方法，传入 zk 的连接地址，如：127.0.0.1:2181
     *
     * @param registryAddress zookeeper 的连接地址
     */
    public ZookeeperServiceRegistry(String registryAddress) {
        try {
            // 创建 Zookeeper 客户端，并配置会话超时、连接超时和指数退避重试策略。
            client = CuratorFrameworkFactory
                    .newClient(registryAddress, SESSION_TIMEOUT, CONNECT_TIMEOUT,
                            new ExponentialBackoffRetry(BASE_SLEEP_TIME, MAX_RETRY));
            // 启动客户端，开始和 Zookeeper 建立连接。
            client.start();

            // 构建 Curator ServiceDiscovery。
            // JsonInstanceSerializer 用于把 ServiceInfo 序列化到 Zookeeper 节点中。
            serviceDiscovery = ServiceDiscoveryBuilder.builder(ServiceInfo.class)
                    .client(client)
                    .serializer(new JsonInstanceSerializer<>(ServiceInfo.class))
                    .basePath(BASE_PATH)
                    .build();

            // 启动服务发现组件，后续才能注册和注销服务实例。
            serviceDiscovery.start();
        } catch (Exception e) {
            log.error("An error occurred while starting the zookeeper registry: ", e);
        }
    }

    /**
     * 注册服务实例到 Zookeeper。
     *
     * RpcServerBeanPostProcessor 会在 @RpcService Bean 初始化完成后调用这个方法。
     * 这里将 ServiceInfo 转成 Curator 认识的 ServiceInstance：
     * name 对应服务唯一名，address 和 port 对应 provider 地址，payload 保存完整服务元数据。
     *
     * @param serviceInfo 服务元信息
     */
    @Override
    public void register(ServiceInfo serviceInfo) {
        try {
            // Curator ServiceDiscovery 使用 ServiceInstance 作为注册单位。
            ServiceInstance<ServiceInfo> serviceInstance = ServiceInstance.<ServiceInfo>builder()
                    .name(serviceInfo.getServiceName())
                    .address(serviceInfo.getAddress())
                    .port(serviceInfo.getPort())
                    .payload(serviceInfo)
                    .build();
            // 将当前 provider 写入 Zookeeper，客户端后续可以通过服务名发现这个节点。
            serviceDiscovery.registerService(serviceInstance);
            log.info("Successfully registered [{}] service.", serviceInstance.getName());
        } catch (Exception e) {
            throw new RpcException(String.format("An error occurred when rpc server registering [%s] service.",
                    serviceInfo.getServiceName()), e);
        }
    }

    /**
     * 从 Zookeeper 注销服务实例。
     *
     * 注销时也需要构造同一个服务实例信息，让 Curator 能定位到对应的注册节点。
     *
     * @param serviceInfo 服务元信息
     */
    @Override
    public void unregister(ServiceInfo serviceInfo) throws Exception {
        ServiceInstance<ServiceInfo> serviceInstance = ServiceInstance.<ServiceInfo>builder()
                .name(serviceInfo.getServiceName())
                .address(serviceInfo.getAddress())
                .port(serviceInfo.getPort())
                .payload(serviceInfo)
                .build();
        serviceDiscovery.unregisterService(serviceInstance);
        log.warn("Successfully unregistered {} service.", serviceInstance.getName());
    }

    /**
     * 关闭注册中心相关资源。
     *
     * 服务端关闭时会调用该方法，先关闭 ServiceDiscovery，再关闭底层 Zookeeper 客户端。
     * 如果注册节点是临时节点，Zookeeper 会在会话结束后清理对应服务实例。
     */
    @Override
    public void destroy() throws Exception {
        serviceDiscovery.close();
        client.close();
        log.info("Destroy zookeeper registry completed.");
    }
}
