package com.wxy.rpc.core.common;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 服务端负载指标。
 *
 * 该对象会随着心跳响应返回给客户端，客户端的自适应负载均衡可以根据这些指标避开高负载节点。
 * CPU 和内存使用率取值范围是 0 到 1，-1 表示当前运行环境无法获取该指标。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ServerLoadMetrics implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 整机 CPU 使用率。
     */
    private double systemCpuLoad;

    /**
     * 当前 Java 进程 CPU 使用率。
     */
    private double processCpuLoad;

    /**
     * JVM 堆内存使用率。
     */
    private double heapMemoryUsage;

    /**
     * CPU 核数。
     */
    private int availableProcessors;

    /**
     * 服务端业务线程池当前活跃线程数。
     */
    private int activeThreadCount;

    /**
     * 服务端业务线程池核心线程数。
     */
    private int corePoolSize;

    /**
     * 服务端业务线程池最大线程数。
     */
    private int maximumPoolSize;

    /**
     * 服务端业务线程池当前排队任务数。
     */
    private int queueSize;

    /**
     * 服务端业务线程池剩余队列容量。
     */
    private int queueRemainingCapacity;

    /**
     * 指标采集时间。
     */
    private long timestamp;
}
