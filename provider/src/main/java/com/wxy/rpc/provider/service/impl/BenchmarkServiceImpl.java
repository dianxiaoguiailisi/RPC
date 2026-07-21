package com.wxy.rpc.provider.service.impl;

import com.wxy.rpc.api.service.BenchmarkService;
import com.wxy.rpc.server.annotation.RpcService;

/**
 * 压测服务实现。
 */
@RpcService(interfaceClass = BenchmarkService.class)
public class BenchmarkServiceImpl implements BenchmarkService {

    @Override
    public String ping(String name) {
        return "pong, " + name;
    }

    @Override
    public String cpuTask(Integer rounds) {
        int count = rounds == null ? 1000 : Math.max(rounds, 1);
        long value = 1125899906842597L;
        for (int i = 0; i < count; i++) {
            value = 31 * value + i;
            value ^= value << 13;
            value ^= value >>> 7;
            value ^= value << 17;
        }
        return Long.toHexString(value);
    }

    @Override
    public String ioTask(Integer millis) {
        int sleepMillis = millis == null ? 10 : Math.max(millis, 0);
        try {
            Thread.sleep(sleepMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return "interrupted";
        }
        return "sleep " + sleepMillis + "ms";
    }
}
