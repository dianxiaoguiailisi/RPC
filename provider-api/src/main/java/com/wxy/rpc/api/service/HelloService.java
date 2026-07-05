package com.wxy.rpc.api.service;

import java.util.concurrent.CompletableFuture;

/**
 * 示例服务接口
 */
public interface HelloService {
    String sayHello(String name);

    CompletableFuture<String> sayHelloAsync(String name);
}
