package com.wxy.rpc.provider;

import com.wxy.rpc.server.annotation.RpcComponentScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
/**
 * 服务端 Spring Boot 启动类，使用 `@RpcComponentScan` 扫描 RPC 服务实现。
 */
@SpringBootApplication
@RpcComponentScan(basePackages = {"com.wxy.rpc.provider"})
public class ProviderApplication {
    /** 
     * @param args
     */
    public static void main(String[] args) {
        SpringApplication.run(ProviderApplication.class, args);
    }
}
