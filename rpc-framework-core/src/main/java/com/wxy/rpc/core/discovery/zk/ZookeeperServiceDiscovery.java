package com.wxy.rpc.core.discovery.zk;

import com.wxy.rpc.core.common.RpcRequest;
import com.wxy.rpc.core.common.ServiceInfo;
import com.wxy.rpc.core.discovery.ServiceDiscovery;
import com.wxy.rpc.core.exception.RpcException;
import com.wxy.rpc.core.loadbalance.LoadBalance;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.discovery.ServiceCache;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.apache.curator.x.discovery.details.ServiceCacheListener;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 基于 Zookeeper 的服务发现实现类。
 *
 * 这个类运行在消费端，用来根据 serviceName 查询可用的服务提供者列表。它不是每次 RPC 调用都直接访问 Zookeeper，而是使用 Curator 的 ServiceCache
 * 将服务列表缓存到本地，并监听 Zookeeper 中服务节点的变化。
 *
 * 调用流程：
 * 1. 客户端发起远程调用时，RemoteMethodCall 会构造 RpcRequest。
 * 2. 根据 RpcRequest 中的 serviceName 调用 discover。
 * 3. discover 会通过 getServices 获取本地缓存中的服务列表。
 * 4. 如果是第一次查询某个 serviceName，会创建 ServiceCache 并从 Zookeeper 拉取服务列表。
 * 5. 后续服务节点变化时，ServiceCacheListener 会自动更新本地缓存。
 * 6. 最后通过负载均衡策略从服务列表中选择一个 provider。
 */
@Slf4j
public class ZookeeperServiceDiscovery implements ServiceDiscovery {
    /**
     * Zookeeper 会话超时时间。
     */
    private static final int SESSION_TIMEOUT = 60 * 1000;

    /**
     * Zookeeper 连接超时时间。
     */
    private static final int CONNECT_TIMEOUT = 15 * 1000;

    /**
     * 重试初始等待时间。
     */
    private static final int BASE_SLEEP_TIME = 3 * 1000;

    /**
     * 最大重试次数。
     */
    private static final int MAX_RETRY = 10;

    /**
     * RPC 服务在 Zookeeper 中的根路径。服务注册和服务发现需要使用同一个根路径，否则客户端发现不到服务端注册的节点。
     */
    private static final String BASE_PATH = "/wxy_rpc";

    /**
     * 负载均衡策略。当同一个服务有多个 provider 时，通过该策略选择一个具体服务节点。
     */
    private LoadBalance loadBalance;

    /**
     * Curator 客户端，负责和 Zookeeper 建立连接。
     */
    private CuratorFramework client;

    /**
     * Curator 提供的服务发现对象。
     * 它基于指定的 BASE_PATH 读取服务实例信息，并将 JSON 数据反序列化为 ServiceInfo。
     */
    private org.apache.curator.x.discovery.ServiceDiscovery<ServiceInfo> serviceDiscovery;

    /**
     * ServiceCache 缓存表。
     *
     * key 是 serviceName，value 是 Curator 的 ServiceCache。
     * 每个 serviceName 会对应一个 ServiceCache，用来监听这个服务下 provider 节点的变化。
     *
     * ServiceCache 的作用：
     * 1. 将 Zookeeper 中的服务实例缓存到本地。
     * 2. 监听服务实例变化。
     * 3. 当 provider 上线或下线时，触发 cacheChanged 回调。
     */
    private final Map<String, ServiceCache<ServiceInfo>> serviceCacheMap = new ConcurrentHashMap<>();

    /**
     * 本地服务列表缓存。
     * key 是 serviceName，value 是当前可用的服务实例列表。
     * RPC 调用时优先从这里获取服务列表，再交给负载均衡选择节点。
     * 这个缓存还有一个作用：当 Zookeeper 临时不可用时，客户端仍然保留最后一次成功拉取到的服务列表，
     * 在 provider 没有真实下线的情况下，可以继续使用已有地址发起调用。
     */
    private final Map<String, List<ServiceInfo>> serviceMap = new ConcurrentHashMap<>();


    /**
     * 创建 Zookeeper 服务发现对象。
     *
     * 初始化时会完成：
     * 1. 保存负载均衡策略。
     * 2. 创建 CuratorFramework 客户端。
     * 3. 启动 Zookeeper 连接。
     * 4. 创建 Curator ServiceDiscovery 对象。
     * 5. 启动服务发现能力。
     *
     * @param registryAddress zookeeper 的连接地址
     * @param loadBalance 负载均衡策略
     */
    public ZookeeperServiceDiscovery(String registryAddress, LoadBalance loadBalance) {
        try {
            this.loadBalance = loadBalance;

            // 创建zk客户端示例
            client = CuratorFrameworkFactory
                    .newClient(registryAddress, SESSION_TIMEOUT, CONNECT_TIMEOUT,
                            new ExponentialBackoffRetry(BASE_SLEEP_TIME, MAX_RETRY));
            // 开启客户端通信
            client.start();

            // 构建 ServiceDiscovery 服务注册中心
            serviceDiscovery = ServiceDiscoveryBuilder.builder(ServiceInfo.class)
                    .client(client)
                    .serializer(new JsonInstanceSerializer<>(ServiceInfo.class))
                    .basePath(BASE_PATH)
                    .build();
            // 开启 服务发现
            serviceDiscovery.start();
        } catch (Exception e) {
            log.error("An error occurred while starting the zookeeper discovery: ", e);
        }
    }

    /**
     * 根据 RPC 请求发现一个可用服务实例。这里并不是简单返回第一个服务实例，而是先根据 serviceName 获取服务列表，
     * 再通过负载均衡策略选择一个具体 provider。
     * @param request RPC 请求，里面包含 serviceName、方法名、参数等信息
     * @return 负载均衡选中的服务实例
     */
    @Override
    public ServiceInfo discover(RpcRequest request) {

        try {
            return loadBalance.select(getServices(request.getServiceName()), request);
        } catch (Exception e) {
            throw new RpcException(String.format("Remote service discovery did not find service %s.",
                    request.getServiceName()), e);
        }
    }

    /**
     * 获取指定服务的 provider 列表。
     * 如果本地已经缓存过该 serviceName，直接返回 serviceMap 中的列表。如果是第一次查询该 serviceName，则创建 ServiceCache：
     * 1. 从 Zookeeper 拉取当前服务实例列表。
     * 2. 将服务实例列表缓存到 serviceMap。
     * 3. 注册 ServiceCacheListener，后续 provider 上线或下线时自动更新 serviceMap。
     *
     * @param serviceName 服务唯一名称，通常是 接口全限定名-版本号
     * @return 当前可用的服务实例列表
     * @throws Exception 创建或启动 ServiceCache 失败时抛出
     */
    @Override
    public List<ServiceInfo> getServices(String serviceName) throws Exception {
        if (!serviceMap.containsKey(serviceName)) {
            // 构建本地服务缓存
            ServiceCache<ServiceInfo> serviceCache = serviceDiscovery.serviceCacheBuilder()
                    .name(serviceName)
                    .build();
            // 添加服务监听，当服务发生变化时主动更新本地缓存并通知
            serviceCache.addListener(new ServiceCacheListener() {
                @Override
                public void cacheChanged() {
                    log.info("The service [{}] cache has changed. The current number of service samples is {}."
                            , serviceName, serviceCache.getInstances().size());
                    // provider 节点发生变化时，重新读取 ServiceCache 中的实例，并刷新本地 serviceMap。
                    serviceMap.put(serviceName, serviceCache.getInstances().stream()
                            .map(ServiceInstance::getPayload)
                            .collect(Collectors.toList()));
                }

                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState) {
                    // Zookeeper 连接状态变化时只记录日志，不主动清空本地服务列表。
                    // 这样 Zookeeper 短暂不可用时，客户端仍然可以使用上一次缓存的 provider 地址。
                    log.info("The client {} connection status has changed. The current status is: {}."
                            , client, newState);
                }
            });
            // 开启服务缓存监听
            serviceCache.start();
            // 将服务缓存对象存入本地
            serviceCacheMap.put(serviceName, serviceCache);
            // 第一次查询时，将当前从 Zookeeper 获取到的服务实例列表缓存到本地 serviceMap。
            serviceMap.put(serviceName, serviceCacheMap.get(serviceName).getInstances()
                    .stream()
                    .map(ServiceInstance::getPayload)
                    .collect(Collectors.toList()));
        }
        return serviceMap.get(serviceName);
    }

    /**
     * 销毁服务发现资源。
     * 客户端关闭时需要关闭所有 ServiceCache、ServiceDiscovery 和 Curator 客户端，
     * 避免监听器、Zookeeper 连接等资源泄漏。
     *
     * @throws Exception 关闭资源失败时抛出
     */
    @Override
    public void destroy() throws Exception {
        for (ServiceCache<ServiceInfo> serviceCache : serviceCacheMap.values()) {
            if (serviceCache != null) {
                serviceCache.close();
            }
        }
        if (serviceDiscovery != null) {
            serviceDiscovery.close();
        }
        if (client != null) {
            client.close();
        }
    }
}
