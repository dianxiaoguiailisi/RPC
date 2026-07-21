package com.wxy.rpc.core.metrics;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * RPC 压测阶段耗时采集器。
 *
 * 默认关闭。压测时通过 -Drpc.metrics.enabled=true 开启，并把聚合结果写入 CSV 文件。
 */
@Slf4j
public final class RpcMetricsCollector {

    private static final String ENABLED_PROPERTY = "rpc.metrics.enabled";
    private static final String FILE_PROPERTY = "rpc.metrics.file";
    private static final String FLUSH_INTERVAL_PROPERTY = "rpc.metrics.flush-interval-millis";
    private static final DateTimeFormatter CSV_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final boolean ENABLED = Boolean.parseBoolean(System.getProperty(ENABLED_PROPERTY, "false"));
    private static final Map<MetricKey, ConcurrentLinkedQueue<Long>> METRICS = new ConcurrentHashMap<>();
    private static final ScheduledExecutorService FLUSH_EXECUTOR;
    private static final BufferedWriter WRITER;

    static {
        BufferedWriter writer = null;
        ScheduledExecutorService executor = null;
        if (ENABLED) {
            try {
                String defaultFile = "rpc-metrics-" + new SimpleDateFormat("yyyyMMdd-HHmmss")
                        .format(System.currentTimeMillis()) + ".csv";
                String fileName = System.getProperty(FILE_PROPERTY, defaultFile);
                File file = new File(fileName);
                File parent = file.getParentFile();
                if (parent != null) {
                    Files.createDirectories(parent.toPath());
                }
                writer = Files.newBufferedWriter(file.toPath(), StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                writer.write("timestamp,role,service,method,metric,count,avgMs,p50Ms,p90Ms,p95Ms,p99Ms,maxMs");
                writer.newLine();
                writer.flush();

                long flushIntervalMillis = Long.parseLong(System.getProperty(FLUSH_INTERVAL_PROPERTY, "5000"));
                executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
                    Thread thread = new Thread(runnable, "rpc-metrics-flusher");
                    thread.setDaemon(true);
                    return thread;
                });
                executor.scheduleAtFixedRate(RpcMetricsCollector::flushSafely, flushIntervalMillis,
                        flushIntervalMillis, TimeUnit.MILLISECONDS);
                Runtime.getRuntime().addShutdownHook(new Thread(RpcMetricsCollector::shutdown,
                        "rpc-metrics-shutdown"));
            } catch (Exception e) {
                log.warn("RPC metrics collector initialization failed.", e);
                closeQuietly(writer);
                writer = null;
                if (executor != null) {
                    executor.shutdownNow();
                    executor = null;
                }
            }
        }
        WRITER = writer;
        FLUSH_EXECUTOR = executor;
    }

    private RpcMetricsCollector() {
    }

    public static boolean isEnabled() {
        return ENABLED && WRITER != null;
    }

    public static long now() {
        return System.nanoTime();
    }

    public static void record(String role, String service, String method, String metric, long costNanos) {
        if (!isEnabled() || costNanos < 0) {
            return;
        }
        MetricKey key = new MetricKey(role, normalize(service), normalize(method), normalize(metric));
        METRICS.computeIfAbsent(key, ignored -> new ConcurrentLinkedQueue<>()).add(costNanos);
    }

    public static void record(String role, RpcMetricsContext.InvocationInfo info, String metric, long costNanos) {
        if (info == null) {
            record(role, "unknown", "unknown", metric, costNanos);
            return;
        }
        record(role, info.getServiceName(), info.getMethodName(), metric, costNanos);
    }

    public static void recordSince(String role, String service, String method, String metric, long startNanos) {
        record(role, service, method, metric, now() - startNanos);
    }

    public static void recordSince(String role, RpcMetricsContext.InvocationInfo info, String metric, long startNanos) {
        record(role, info, metric, now() - startNanos);
    }

    public static void flush() {
        if (!isEnabled()) {
            return;
        }
        String timestamp = CSV_TIME_FORMATTER.format(LocalDateTime.now());
        synchronized (WRITER) {
            for (Map.Entry<MetricKey, ConcurrentLinkedQueue<Long>> entry : METRICS.entrySet()) {
                List<Long> values = drain(entry.getValue());
                if (values.isEmpty()) {
                    continue;
                }
                writeMetric(timestamp, entry.getKey(), values);
            }
            try {
                WRITER.flush();
            } catch (IOException e) {
                log.warn("Flush RPC metrics file failed.", e);
            }
        }
    }

    public static void shutdown() {
        if (FLUSH_EXECUTOR != null) {
            FLUSH_EXECUTOR.shutdown();
        }
        flush();
        closeQuietly(WRITER);
    }

    private static void flushSafely() {
        try {
            flush();
        } catch (Exception e) {
            log.warn("Flush RPC metrics failed.", e);
        }
    }

    private static List<Long> drain(ConcurrentLinkedQueue<Long> queue) {
        List<Long> values = new ArrayList<>(queue.size());
        Long value;
        while ((value = queue.poll()) != null) {
            values.add(value);
        }
        return values;
    }

    private static void writeMetric(String timestamp, MetricKey key, List<Long> values) {
        Collections.sort(values);
        int count = values.size();
        long sum = 0L;
        for (Long value : values) {
            sum += value;
        }
        long avg = sum / count;
        long p50 = percentile(values, 0.50);
        long p90 = percentile(values, 0.90);
        long p95 = percentile(values, 0.95);
        long p99 = percentile(values, 0.99);
        long max = values.get(count - 1);
        try {
            WRITER.write(timestamp);
            WRITER.write(',');
            WRITER.write(key.role);
            WRITER.write(',');
            WRITER.write(key.service);
            WRITER.write(',');
            WRITER.write(key.method);
            WRITER.write(',');
            WRITER.write(key.metric);
            WRITER.write(',');
            WRITER.write(Integer.toString(count));
            WRITER.write(',');
            WRITER.write(formatMillis(avg));
            WRITER.write(',');
            WRITER.write(formatMillis(p50));
            WRITER.write(',');
            WRITER.write(formatMillis(p90));
            WRITER.write(',');
            WRITER.write(formatMillis(p95));
            WRITER.write(',');
            WRITER.write(formatMillis(p99));
            WRITER.write(',');
            WRITER.write(formatMillis(max));
            WRITER.newLine();
        } catch (IOException e) {
            log.warn("Write RPC metrics file failed.", e);
        }
    }

    private static long percentile(List<Long> values, double percentile) {
        int index = (int) Math.ceil(values.size() * percentile) - 1;
        index = Math.max(0, Math.min(index, values.size() - 1));
        return values.get(index);
    }

    private static String formatMillis(long nanos) {
        BigDecimal millis = BigDecimal.valueOf(nanos)
                .divide(BigDecimal.valueOf(1_000_000L), 3, RoundingMode.HALF_UP);
        return millis.toPlainString();
    }

    private static String normalize(String value) {
        if (value == null || value.trim().isEmpty()) {
            return "unknown";
        }
        return value.replace(',', ';');
    }

    private static void closeQuietly(BufferedWriter writer) {
        if (writer == null) {
            return;
        }
        try {
            writer.close();
        } catch (IOException ignored) {
        }
    }

    private static final class MetricKey {
        private final String role;
        private final String service;
        private final String method;
        private final String metric;

        private MetricKey(String role, String service, String method, String metric) {
            this.role = normalize(role);
            this.service = service;
            this.method = method;
            this.metric = metric;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof MetricKey)) {
                return false;
            }
            MetricKey metricKey = (MetricKey) o;
            return Objects.equals(role, metricKey.role)
                    && Objects.equals(service, metricKey.service)
                    && Objects.equals(method, metricKey.method)
                    && Objects.equals(metric, metricKey.metric);
        }

        @Override
        public int hashCode() {
            return Objects.hash(role, service, method, metric);
        }
    }
}
