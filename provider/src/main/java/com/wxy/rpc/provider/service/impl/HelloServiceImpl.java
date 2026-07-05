package com.wxy.rpc.provider.service.impl;

import com.wxy.rpc.api.service.HelloService;
import com.wxy.rpc.server.annotation.RpcService;

import java.util.concurrent.CompletableFuture;

/**
 * `HelloServiceImpl`：`HelloService` 的实现类，用 `@RpcService` 暴露成 RPC 服务
 */
@RpcService(interfaceClass = HelloService.class)
public class HelloServiceImpl implements HelloService {
    /** 
     * @param name
     * @return String
     */
    @Override
    public String sayHello(String name) {
        return "Hello, " + name;
    }

    /**
     * 异步 RPC 示例方法。
     *
     * 服务端返回 CompletableFuture 时，RPC Server 不应该把 Future 对象本身返回给客户端，
     * 而是等待 Future 完成后把真正的业务结果写回客户端。
     */
    @Override
    public CompletableFuture<String> sayHelloAsync(String name) {
        return CompletableFuture.supplyAsync(() -> "Hello async, " + name);
    }
}
