package com.wxy.rpc.api.service;

/**
 * 压测服务接口，用来区分 RPC 空载、CPU 型业务和 IO 型业务。
 */
public interface BenchmarkService {

    /**
     * 纯 RPC 空载调用。
     */
    String ping(String name);

    /**
     * 模拟 CPU 计算型业务。
     */
    String cpuTask(Integer rounds);

    /**
     * 模拟 IO 等待型业务。
     */
    String ioTask(Integer millis);
}
