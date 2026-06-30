package com.wxy.rpc.server.metrics;

import com.wxy.rpc.core.common.ServerLoadMetrics;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 服务端负载指标采集器。
 *
 * 当前采集机器 CPU、当前 Java 进程 CPU、JVM 堆内存以及 RPC 业务线程池状态。
 */
public class ServerLoadMetricsCollector {

    private static final OperatingSystemMXBean OPERATING_SYSTEM_BEAN =
            ManagementFactory.getOperatingSystemMXBean();

    private static final MemoryMXBean MEMORY_BEAN = ManagementFactory.getMemoryMXBean();

    private ServerLoadMetricsCollector() {
    }

    public static ServerLoadMetrics collect(ThreadPoolExecutor executor) {
        return collect(Collections.singletonList(executor));
    }

    public static ServerLoadMetrics collect(Collection<ThreadPoolExecutor> executors) {
        MemoryUsage heapMemoryUsage = MEMORY_BEAN.getHeapMemoryUsage();
        ThreadPoolSnapshot snapshot = snapshot(executors);
        return ServerLoadMetrics.builder()
                .systemCpuLoad(getSystemCpuLoad())
                .processCpuLoad(getProcessCpuLoad())
                .heapMemoryUsage(getHeapMemoryUsage(heapMemoryUsage))
                .availableProcessors(OPERATING_SYSTEM_BEAN.getAvailableProcessors())
                .activeThreadCount(snapshot.activeThreadCount)
                .corePoolSize(snapshot.corePoolSize)
                .maximumPoolSize(snapshot.maximumPoolSize)
                .queueSize(snapshot.queueSize)
                .queueRemainingCapacity(snapshot.queueRemainingCapacity)
                .timestamp(System.currentTimeMillis())
                .build();
    }

    private static ThreadPoolSnapshot snapshot(Collection<ThreadPoolExecutor> executors) {
        ThreadPoolSnapshot snapshot = new ThreadPoolSnapshot();
        if (executors == null) {
            return snapshot;
        }
        for (ThreadPoolExecutor executor : executors) {
            if (executor == null) {
                continue;
            }
            snapshot.activeThreadCount += executor.getActiveCount();
            snapshot.corePoolSize += executor.getCorePoolSize();
            snapshot.maximumPoolSize += executor.getMaximumPoolSize();
            snapshot.queueSize += executor.getQueue().size();
            snapshot.queueRemainingCapacity += executor.getQueue().remainingCapacity();
        }
        return snapshot;
    }

    private static double getSystemCpuLoad() {
        if (OPERATING_SYSTEM_BEAN instanceof com.sun.management.OperatingSystemMXBean) {
            return normalize(((com.sun.management.OperatingSystemMXBean) OPERATING_SYSTEM_BEAN).getSystemCpuLoad());
        }
        return -1D;
    }

    private static double getProcessCpuLoad() {
        if (OPERATING_SYSTEM_BEAN instanceof com.sun.management.OperatingSystemMXBean) {
            return normalize(((com.sun.management.OperatingSystemMXBean) OPERATING_SYSTEM_BEAN).getProcessCpuLoad());
        }
        return -1D;
    }

    private static double getHeapMemoryUsage(MemoryUsage heapMemoryUsage) {
        long max = heapMemoryUsage.getMax();
        if (max <= 0) {
            max = heapMemoryUsage.getCommitted();
        }
        if (max <= 0) {
            return -1D;
        }
        return normalize((double) heapMemoryUsage.getUsed() / max);
    }

    private static double normalize(double value) {
        if (Double.isNaN(value) || value < 0D) {
            return -1D;
        }
        if (value > 1D) {
            return 1D;
        }
        return value;
    }

    private static class ThreadPoolSnapshot {
        private int activeThreadCount;
        private int corePoolSize;
        private int maximumPoolSize;
        private int queueSize;
        private int queueRemainingCapacity;
    }
}
