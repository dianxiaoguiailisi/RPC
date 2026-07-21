package com.wxy.rpc.client.proxy;

import com.wxy.rpc.core.common.ServiceInfo;
import com.wxy.rpc.core.common.ServerLoadMetrics;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Provider 健康状态管理器。
 *
 * 这个类用于在客户端侧记录 provider 的失败状态，并提供轻量级熔断和半开探测能力。
 *
 * 状态流转：
 * 1. provider 正常时处于健康状态。
 * 2. 调用失败会累计失败次数。
 * 3. 连续失败达到阈值后，provider 会被短时间熔断。
 * 4. 熔断时间内，选择 provider 时会跳过该节点。
 * 5. 熔断时间结束后，允许一个请求进入半开探测。
 * 6. 半开探测成功则恢复健康，失败则继续熔断。
 */
@Slf4j
public class ProviderHealthManager {

    /**
     * provider 健康状态缓存。
     *
     * key 格式：serviceName@address:port。
     */
    private static final ConcurrentMap<String, ProviderStatus> PROVIDER_STATUS_MAP = new ConcurrentHashMap<>();

    /**
     * 服务端负载指标缓存。
     *
     * key 格式：address:port。
     */
    private static final ConcurrentMap<String, ServerLoadStatus> SERVER_LOAD_STATUS_MAP = new ConcurrentHashMap<>();

    /**
     * 服务端负载指标过期时间。
     *
     * 客户端 15 秒写空闲时发送心跳，超过 45 秒没有刷新则认为指标过期，不参与负载均衡评分。
     */
    private static final long SERVER_LOAD_TTL_MILLIS = 45_000L;

    /**
     * 清空客户端记录的 provider 状态。
     *
     * 主要用于同一个 JVM 内连续执行多个独立压测 Trial，避免前一轮的熔断、活动请求数和
     * 响应时间统计污染下一轮结果。
     */
    public static void reset() {
        PROVIDER_STATUS_MAP.clear();
        SERVER_LOAD_STATUS_MAP.clear();
    }

    /**
     * 判断 provider 当前是否可用。
     *
     * 健康节点直接放行。
     * 熔断时间内的节点不放行。
     * 熔断时间结束后，只放行一个半开探测请求。
     *
     * @param serviceInfo 服务节点
     * @return true 表示可以选择该节点
     */
    public static boolean isAvailable(ServiceInfo serviceInfo) {
        ProviderStatus status = PROVIDER_STATUS_MAP.get(providerKey(serviceInfo));
        if (status == null) {
            return true;
        }
        long now = System.currentTimeMillis();
        long openUntil = status.circuitOpenUntil;
        if (openUntil <= 0) {
            return true;
        }
        if (now < openUntil) {
            return false;
        }
        return status.halfOpenProbe.compareAndSet(false, true);
    }

    /**
     * 记录调用成功。
     *
     * 成功后清空失败次数、熔断截止时间和半开探测标记。
     * 慢调用次数由 recordFastCall 单独清理，避免慢调用刚被记录又被成功逻辑清空。
     *
     * @param serviceInfo 服务节点
     */
    public static void recordSuccess(ServiceInfo serviceInfo) {
        ProviderStatus status = PROVIDER_STATUS_MAP.get(providerKey(serviceInfo));
        if (status == null) {
            return;
        }
        status.failureCount.set(0);
        status.circuitOpenUntil = 0;
        status.halfOpenProbe.set(false);
    }

    /**
     * 记录一次调用开始。
     *
     * activeCount 表示当前 provider 上还没有完成的请求数，
     * 自适应负载均衡会优先选择 activeCount 更低的节点。
     *
     * @param serviceInfo 服务节点
     */
    public static void recordCallStart(ServiceInfo serviceInfo) {
        ProviderStatus status = PROVIDER_STATUS_MAP.computeIfAbsent(providerKey(serviceInfo), key -> new ProviderStatus());
        status.activeCount.incrementAndGet();
    }

    /**
     * 记录一次调用结束。
     *
     * 这里会更新 provider 的平均响应时间，并减少 activeCount。
     *
     * @param serviceInfo 服务节点
     * @param elapsedMillis 调用耗时，单位毫秒
     */
    public static void recordCallEnd(ServiceInfo serviceInfo, long elapsedMillis) {
        ProviderStatus status = PROVIDER_STATUS_MAP.computeIfAbsent(providerKey(serviceInfo), key -> new ProviderStatus());
        status.activeCount.updateAndGet(current -> current > 0 ? current - 1 : 0);
        updateAverageResponseTime(status, elapsedMillis);
    }

    /**
     * 记录一次快速成功调用。
     *
     * 快速成功说明当前 provider 响应恢复正常，可以清空连续慢调用计数。
     *
     * @param serviceInfo 服务节点
     */
    public static void recordFastCall(ServiceInfo serviceInfo) {
        ProviderStatus status = PROVIDER_STATUS_MAP.get(providerKey(serviceInfo));
        if (status != null) {
            status.slowCallCount.set(0);
        }
    }

    /**
     * 记录慢调用。
     *
     * 如果某个 provider 连续慢调用达到阈值，也会被临时熔断。
     *
     * @param serviceInfo 服务节点
     * @param slowCallThresholdCount 慢调用阈值次数
     * @param circuitOpenMillis 熔断持续时间，单位毫秒
     */
    public static void recordSlowCall(ServiceInfo serviceInfo, int slowCallThresholdCount, long circuitOpenMillis) {
        ProviderStatus status = PROVIDER_STATUS_MAP.computeIfAbsent(providerKey(serviceInfo), key -> new ProviderStatus());
        int threshold = Math.max(slowCallThresholdCount, 1);
        long openMillis = Math.max(circuitOpenMillis, 1L);
        int slowCalls = status.slowCallCount.incrementAndGet();
        if (slowCalls >= threshold) {
            status.circuitOpenUntil = System.currentTimeMillis() + openMillis;
            status.halfOpenProbe.set(false);
            log.warn("Provider [{}:{}] for service [{}] is temporarily isolated by slow calls, slowCallCount: {}, openMillis: {}.",
                    serviceInfo.getAddress(), serviceInfo.getPort(), serviceInfo.getServiceName(), slowCalls, openMillis);
        }
    }

    /**
     * 记录调用失败。
     *
     * 如果连续失败次数达到阈值，或者当前节点处于半开探测状态，则熔断该 provider。
     *
     * @param serviceInfo 服务节点
     * @param failureThreshold 失败阈值
     * @param circuitOpenMillis 熔断持续时间，单位毫秒
     */
    public static void recordFailure(ServiceInfo serviceInfo, int failureThreshold, long circuitOpenMillis) {
        ProviderStatus status = PROVIDER_STATUS_MAP.computeIfAbsent(providerKey(serviceInfo), key -> new ProviderStatus());
        int threshold = Math.max(failureThreshold, 1);
        long openMillis = Math.max(circuitOpenMillis, 1L);
        boolean halfOpen = status.halfOpenProbe.get();
        int failures = status.failureCount.incrementAndGet();
        if (halfOpen || failures >= threshold) {
            status.circuitOpenUntil = System.currentTimeMillis() + openMillis;
            status.halfOpenProbe.set(false);
            log.warn("Provider [{}:{}] for service [{}] is temporarily isolated, failureCount: {}, openMillis: {}.",
                    serviceInfo.getAddress(), serviceInfo.getPort(), serviceInfo.getServiceName(), failures, openMillis);
        }
    }

    /**
     * 更新服务端通过心跳响应上报的负载指标。
     *
     * @param address 服务端地址
     * @param port 服务端端口
     * @param metrics 服务端负载指标
     */
    public static void updateServerLoadMetrics(String address, int port, ServerLoadMetrics metrics) {
        if (address == null || address.trim().isEmpty() || metrics == null) {
            return;
        }
        SERVER_LOAD_STATUS_MAP.put(addressKey(address, port),
                new ServerLoadStatus(metrics, System.currentTimeMillis()));
    }

    /**
     * 计算 provider 当前综合得分。
     *
     * 分数越低，代表该节点当前越适合接收新请求。
     * 这里综合考虑了客户端观测指标和服务端心跳上报指标。
     *
     * @param serviceInfo 服务节点
     * @return provider 当前得分
     */
    public static long score(ServiceInfo serviceInfo) {
        ProviderStatus status = PROVIDER_STATUS_MAP.get(providerKey(serviceInfo));
        long clientObservedScore = 0L;
        long now = System.currentTimeMillis();
        if (status != null) {
            if (status.circuitOpenUntil > now) {
                return Long.MAX_VALUE;
            }
            long activeScore = (long) status.activeCount.get() * 1000L;
            long responseTimeScore = Math.max(status.averageResponseMillis, 0L);
            long failureScore = (long) status.failureCount.get() * 500L;
            long slowCallScore = (long) status.slowCallCount.get() * 300L;
            long halfOpenScore = status.halfOpenProbe.get() ? 10000L : 0L;
            clientObservedScore = activeScore + responseTimeScore + failureScore + slowCallScore + halfOpenScore;
        }
        return clientObservedScore + serverLoadScore(serviceInfo, now);
    }

    private static void updateAverageResponseTime(ProviderStatus status, long elapsedMillis) {
        long elapsed = Math.max(elapsedMillis, 0L);
        long current = status.averageResponseMillis;
        if (current <= 0) {
            status.averageResponseMillis = elapsed;
            return;
        }
        status.averageResponseMillis = current * 7 / 10 + elapsed * 3 / 10;
    }

    private static String providerKey(ServiceInfo serviceInfo) {
        return serviceInfo.getServiceName() + "@" + serviceInfo.getAddress() + ":" + serviceInfo.getPort();
    }

    private static String addressKey(String address, Integer port) {
        return address + ":" + port;
    }

    private static long serverLoadScore(ServiceInfo serviceInfo, long now) {
        ServerLoadStatus loadStatus = SERVER_LOAD_STATUS_MAP.get(addressKey(serviceInfo.getAddress(), serviceInfo.getPort()));
        if (loadStatus == null || now - loadStatus.receiveTime > SERVER_LOAD_TTL_MILLIS) {
            return 0L;
        }
        ServerLoadMetrics metrics = loadStatus.metrics;
        long systemCpuScore = ratioScore(metrics.getSystemCpuLoad(), 2000L);
        long processCpuScore = ratioScore(metrics.getProcessCpuLoad(), 1000L);
        long heapScore = ratioScore(metrics.getHeapMemoryUsage(), 500L);
        long threadPoolScore = threadPoolScore(metrics);
        long queueScore = queueScore(metrics);
        return systemCpuScore + processCpuScore + heapScore + threadPoolScore + queueScore;
    }

    private static long ratioScore(double ratio, long weight) {
        if (Double.isNaN(ratio) || ratio < 0D) {
            return 0L;
        }
        return (long) (Math.min(ratio, 1D) * weight);
    }

    private static long threadPoolScore(ServerLoadMetrics metrics) {
        int maximumPoolSize = metrics.getMaximumPoolSize();
        if (maximumPoolSize <= 0) {
            return 0L;
        }
        double ratio = (double) metrics.getActiveThreadCount() / maximumPoolSize;
        return ratioScore(ratio, 1000L);
    }

    private static long queueScore(ServerLoadMetrics metrics) {
        int queueSize = metrics.getQueueSize();
        int totalCapacity = queueSize + metrics.getQueueRemainingCapacity();
        if (totalCapacity <= 0) {
            return 0L;
        }
        double ratio = (double) queueSize / totalCapacity;
        return ratioScore(ratio, 1500L);
    }

    private static class ProviderStatus {
        private final AtomicInteger activeCount = new AtomicInteger();
        private final AtomicInteger failureCount = new AtomicInteger();
        private final AtomicInteger slowCallCount = new AtomicInteger();
        private final AtomicBoolean halfOpenProbe = new AtomicBoolean(false);
        private volatile long circuitOpenUntil;
        private volatile long averageResponseMillis;
    }

    private static class ServerLoadStatus {
        private final ServerLoadMetrics metrics;
        private final long receiveTime;

        private ServerLoadStatus(ServerLoadMetrics metrics, long receiveTime) {
            this.metrics = metrics;
            this.receiveTime = receiveTime;
        }
    }
}
