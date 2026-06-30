package com.wxy.rpc.provider.service.impl;

import com.wxy.rpc.api.service.HelloService;
import com.wxy.rpc.server.annotation.RpcService;
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
}
