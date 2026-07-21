package com.wxy.rpc.core.metrics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 通过 sequenceId 关联一次 RPC 调用的服务名、方法名和客户端开始时间。
 */
public final class RpcMetricsContext {

    private static final Map<Integer, InvocationInfo> INVOCATION_MAP = new ConcurrentHashMap<>();

    private RpcMetricsContext() {
    }

    public static void register(int sequenceId, String serviceName, String methodName, long startNanos) {
        if (!RpcMetricsCollector.isEnabled()) {
            return;
        }
        INVOCATION_MAP.put(sequenceId, new InvocationInfo(serviceName, methodName, startNanos));
    }

    public static InvocationInfo get(int sequenceId) {
        if (!RpcMetricsCollector.isEnabled()) {
            return null;
        }
        return INVOCATION_MAP.get(sequenceId);
    }

    public static InvocationInfo remove(int sequenceId) {
        if (!RpcMetricsCollector.isEnabled()) {
            return null;
        }
        return INVOCATION_MAP.remove(sequenceId);
    }

    public static final class InvocationInfo {
        private final String serviceName;
        private final String methodName;
        private final long startNanos;

        private InvocationInfo(String serviceName, String methodName, long startNanos) {
            this.serviceName = serviceName == null ? "unknown" : serviceName;
            this.methodName = methodName == null ? "unknown" : methodName;
            this.startNanos = startNanos;
        }

        public String getServiceName() {
            return serviceName;
        }

        public String getMethodName() {
            return methodName;
        }

        public long getStartNanos() {
            return startNanos;
        }
    }
}
