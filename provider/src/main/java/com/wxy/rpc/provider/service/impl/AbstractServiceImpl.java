package com.wxy.rpc.provider.service.impl;

import com.wxy.rpc.api.service.AbstractService;
import com.wxy.rpc.server.annotation.RpcService;

/**
 * `AbstractServiceImpl`：`AbstractService` 的实现类，同样用 `@RpcService` 注册。
 */
@RpcService(interfaceClass = AbstractService.class)
public class AbstractServiceImpl extends AbstractService {
    /** 
     * @param name
     * @return String
     */
    @Override
    public String abstractHello(String name) {
        return "abstract hello " + name;
    }
}
