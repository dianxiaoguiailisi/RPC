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
 * <p>Netty EventLoop 只负责网络 I/O 和请求解析，真正的服务方法调用会提交到
 * 当前管理器维护的业务线程池中，避免耗时业务阻塞 Netty I/O 线程。</p>
 *
 * <p>默认情况下，所有服务共用 {@link #defaultExecutor}。开启服务级隔离后，
 * 配置了独立线程池的 serviceName 使用对应的隔离线程池，未配置的服务仍回退到默认线程池。
 * 这样可以防止某个服务的慢请求或突发流量耗尽全部业务线程。</p>
 */
@Slf4j
public class BusinessThreadPoolManager {

    /** 默认核心线程数。 */
    private static final int DEFAULT_CORE_POOL_SIZE = 10;

    /** 默认最大线程数。 */
    private static final int DEFAULT_MAX_POOL_SIZE = 10;

    /** 默认有界任务队列容量。 */
    private static final int DEFAULT_QUEUE_CAPACITY = 10000;

    /** 默认非核心线程空闲存活时间，单位为秒。 */
    private static final long DEFAULT_KEEP_ALIVE_SECONDS = 60L;

    /**
     * 全局默认业务线程池。
     *
     * <p>使用 volatile 保证重新初始化后，其他线程能立即看到新的线程池引用。</p>
     */
    private static volatile ThreadPoolExecutor defaultExecutor = createExecutor("rpc-biz-default",
            DEFAULT_CORE_POOL_SIZE, DEFAULT_MAX_POOL_SIZE, DEFAULT_QUEUE_CAPACITY, DEFAULT_KEEP_ALIVE_SECONDS);

    /**
     * 服务名与隔离业务线程池的映射。
     * Key 为完整 serviceName，Value 为该服务独享的线程池。
     */
    private static final ConcurrentMap<String, ThreadPoolExecutor> serviceExecutors = new ConcurrentHashMap<>();

    /** 工具类不允许实例化。 */
    private BusinessThreadPoolManager() {
    }

    /**
     * 根据服务端配置初始化默认业务线程池和服务级隔离线程池。
     *
     * <p>初始化前会关闭上一组线程池，避免 Spring 容器重新创建时残留旧的线程资源。
     * 当隔离开关关闭时只创建默认线程池；开启后，再为每个有效的服务配置创建独立线程池。</p>
     *
     * <p>方法使用 synchronized，避免初始化与关闭过程并发执行。</p>
     *
     * @param properties 服务端配置
     */
    public static synchronized void init(RpcServerProperties properties) {
        // 清理旧线程池，避免重复初始化导致线程资源泄漏。
        shutdown();
        // 配置缺失或非法时使用内置默认值，并保证最大线程数不小于核心线程数。
        int defaultCorePoolSize = positive(properties.getBizCorePoolSize(), DEFAULT_CORE_POOL_SIZE);
        int defaultMaxPoolSize = maxPoolSize(properties.getBizMaxPoolSize(), defaultCorePoolSize);
        int defaultQueueCapacity = positive(properties.getBizQueueCapacity(), DEFAULT_QUEUE_CAPACITY);
        long defaultKeepAliveSeconds = positive(properties.getBizKeepAliveSeconds(), DEFAULT_KEEP_ALIVE_SECONDS);
        defaultExecutor = createExecutor("rpc-biz-default", defaultCorePoolSize, defaultMaxPoolSize,
                defaultQueueCapacity, defaultKeepAliveSeconds);

        // 未开启隔离时，所有服务共用默认业务线程池。
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
        // 为每个配置的 serviceName 创建独立的有界业务线程池。
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
     * <p>如果存在与 serviceName 完全匹配的隔离线程池，则优先使用；
     * 否则回退到全局默认线程池。心跳等没有 serviceName 的消息也使用默认线程池。</p>
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
     * <p>返回的集合包含默认线程池和所有服务级隔离线程池，
     * 可用于汇总活跃线程数、队列长度等负载信息。</p>
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
     *
     * <p>调用 {@link ThreadPoolExecutor#shutdown()} 后不再接收新任务，但会继续处理已提交的任务。
     * 最后清空服务与隔离线程池的映射。</p>
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

    /**
     * 创建业务线程池。
     *
     * <p>使用有界 {@link ArrayBlockingQueue} 限制积压请求数，避免无界队列在高流量下耗尽内存。
     * 当线程数和队列都达到上限时，{@link ThreadPoolExecutor.AbortPolicy} 抛出拒绝异常，
     * 由上层请求处理器向客户端返回明确的失败响应。</p>
     */
    private static ThreadPoolExecutor createExecutor(String threadNamePrefix, int corePoolSize, int maxPoolSize,
                                                     int queueCapacity, long keepAliveSeconds) {
        return new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveSeconds, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(queueCapacity), new NamedThreadFactory(threadNamePrefix),
                new ThreadPoolExecutor.AbortPolicy());
    }

    /** 返回有效的正整数配置，配置为 null 或小于等于 0 时使用默认值。 */
    private static int positive(Integer value, int defaultValue) {
        return value == null || value <= 0 ? defaultValue : value;
    }

    /** 返回有效的正长整数配置，配置非法时使用默认值。 */
    private static long positive(Long value, long defaultValue) {
        return value == null || value <= 0L ? defaultValue : value;
    }

    /** 校正最大线程数，保证其不小于核心线程数。 */
    private static int maxPoolSize(Integer value, int corePoolSize) {
        int maxPoolSize = positive(value, corePoolSize);
        return Math.max(maxPoolSize, corePoolSize);
    }

    /** 判断字符串是否为 null、空字符串或只包含空白字符。 */
    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    /** 将服务名中不适合用于线程名的字符替换为下划线。 */
    private static String sanitize(String serviceName) {
        return serviceName.replaceAll("[^a-zA-Z0-9_.-]", "_");
    }

    /**
     * 业务线程命名工厂，为默认线程池和隔离线程池生成可识别的线程名。
     * 例如：{@code rpc-biz-default-1}。
     */
    private static class NamedThreadFactory implements ThreadFactory {
        /** 线程名前缀。 */
        private final String threadNamePrefix;

        /** 当前线程池内的线程序号生成器。 */
        private final AtomicInteger index = new AtomicInteger(1);

        private NamedThreadFactory(String threadNamePrefix) {
            this.threadNamePrefix = threadNamePrefix;
        }

        /**
         * 创建业务线程并设置包含线程池来源和序号的名称，便于日志查询和问题定位。
         */
        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable);
            thread.setName(threadNamePrefix + "-" + index.getAndIncrement());
            return thread;
        }
    }
}
