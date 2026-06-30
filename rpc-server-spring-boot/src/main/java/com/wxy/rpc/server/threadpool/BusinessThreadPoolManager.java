package com.wxy.rpc.server.threadpool;

import com.wxy.rpc.server.config.RpcServerProperties;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * RPC 服务端业务线程池管理器。
 *
 * 默认情况下，所有服务共用 default 业务线程池。
 * 开启服务级隔离后，只有配置了独立线程池的 serviceName 才会走隔离线程池。
 */
@Slf4j
public class BusinessThreadPoolManager {

    private static final int DEFAULT_CORE_POOL_SIZE = 10;

    private static final int DEFAULT_MAX_POOL_SIZE = 10;

    private static final int DEFAULT_QUEUE_CAPACITY = 10000;

    private static final long DEFAULT_KEEP_ALIVE_SECONDS = 60L;

    private static volatile ThreadPoolExecutor defaultExecutor = createExecutor("rpc-biz-default",
            DEFAULT_CORE_POOL_SIZE, DEFAULT_MAX_POOL_SIZE, DEFAULT_QUEUE_CAPACITY, DEFAULT_KEEP_ALIVE_SECONDS);

    private static final ConcurrentMap<String, ThreadPoolExecutor> serviceExecutors = new ConcurrentHashMap<>();

    private BusinessThreadPoolManager() {
    }

    /**
     * 根据服务端配置初始化默认业务线程池和服务级隔离线程池。
     *
     * @param properties 服务端配置
     */
    public static synchronized void init(RpcServerProperties properties) {
        shutdown();
        int defaultCorePoolSize = positive(properties.getBizCorePoolSize(), DEFAULT_CORE_POOL_SIZE);
        int defaultMaxPoolSize = maxPoolSize(properties.getBizMaxPoolSize(), defaultCorePoolSize);
        int defaultQueueCapacity = positive(properties.getBizQueueCapacity(), DEFAULT_QUEUE_CAPACITY);
        long defaultKeepAliveSeconds = positive(properties.getBizKeepAliveSeconds(), DEFAULT_KEEP_ALIVE_SECONDS);
        defaultExecutor = createExecutor("rpc-biz-default", defaultCorePoolSize, defaultMaxPoolSize,
                defaultQueueCapacity, defaultKeepAliveSeconds);

        if (!Boolean.TRUE.equals(properties.getBizIsolationEnabled())) {
            log.info("RPC service-level business thread pool isolation is disabled.");
            return;
        }
        List<RpcServerProperties.ServiceThreadPoolProperties> isolatedServices =
                properties.getBizIsolationServices();
        if (isolatedServices == null || isolatedServices.isEmpty()) {
            log.info("RPC service-level business thread pool isolation is enabled, but no isolated service is configured.");
            return;
        }
        for (RpcServerProperties.ServiceThreadPoolProperties serviceConfig : isolatedServices) {
            if (serviceConfig == null || isBlank(serviceConfig.getServiceName())) {
                continue;
            }
            String serviceName = serviceConfig.getServiceName().trim();
            int corePoolSize = positive(serviceConfig.getCorePoolSize(), defaultCorePoolSize);
            int maxPoolSize = maxPoolSize(serviceConfig.getMaxPoolSize(), corePoolSize);
            int queueCapacity = positive(serviceConfig.getQueueCapacity(), defaultQueueCapacity);
            long keepAliveSeconds = positive(serviceConfig.getKeepAliveSeconds(), defaultKeepAliveSeconds);
            serviceExecutors.put(serviceName, createExecutor("rpc-biz-" + sanitize(serviceName),
                    corePoolSize, maxPoolSize, queueCapacity, keepAliveSeconds));
            log.info("RPC service [{}] uses isolated business thread pool, core: {}, max: {}, queue: {}.",
                    serviceName, corePoolSize, maxPoolSize, queueCapacity);
        }
    }

    /**
     * 根据 serviceName 选择业务线程池。
     *
     * @param serviceName 服务名
     * @return 业务线程池
     */
    public static ThreadPoolExecutor selectExecutor(String serviceName) {
        if (serviceName != null) {
            ThreadPoolExecutor serviceExecutor = serviceExecutors.get(serviceName);
            if (serviceExecutor != null) {
                return serviceExecutor;
            }
        }
        return defaultExecutor;
    }

    /**
     * 获取所有业务线程池，用于服务端负载指标统计。
     *
     * @return 业务线程池集合
     */
    public static Collection<ThreadPoolExecutor> getExecutors() {
        List<ThreadPoolExecutor> executors = new ArrayList<>(serviceExecutors.size() + 1);
        executors.add(defaultExecutor);
        executors.addAll(serviceExecutors.values());
        return executors;
    }

    /**
     * 关闭所有业务线程池。
     */
    public static synchronized void shutdown() {
        if (defaultExecutor != null) {
            defaultExecutor.shutdown();
        }
        for (ThreadPoolExecutor executor : serviceExecutors.values()) {
            executor.shutdown();
        }
        serviceExecutors.clear();
    }

    private static ThreadPoolExecutor createExecutor(String threadNamePrefix, int corePoolSize, int maxPoolSize,
                                                     int queueCapacity, long keepAliveSeconds) {
        return new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveSeconds, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(queueCapacity), new NamedThreadFactory(threadNamePrefix),
                new ThreadPoolExecutor.AbortPolicy());
    }

    private static int positive(Integer value, int defaultValue) {
        return value == null || value <= 0 ? defaultValue : value;
    }

    private static long positive(Long value, long defaultValue) {
        return value == null || value <= 0L ? defaultValue : value;
    }

    private static int maxPoolSize(Integer value, int corePoolSize) {
        int maxPoolSize = positive(value, corePoolSize);
        return Math.max(maxPoolSize, corePoolSize);
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static String sanitize(String serviceName) {
        return serviceName.replaceAll("[^a-zA-Z0-9_.-]", "_");
    }

    private static class NamedThreadFactory implements ThreadFactory {
        private final String threadNamePrefix;
        private final AtomicInteger index = new AtomicInteger(1);

        private NamedThreadFactory(String threadNamePrefix) {
            this.threadNamePrefix = threadNamePrefix;
        }

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable);
            thread.setName(threadNamePrefix + "-" + index.getAndIncrement());
            return thread;
        }
    }
}
