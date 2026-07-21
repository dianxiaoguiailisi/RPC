package com.wxy.rpc.core.common;

import lombok.Data;

import java.io.Serializable;

/**
 * Rpc 请求消息实体类
 */
@Data
public class RpcRequest implements Serializable {

    /**
     * 服务名称：请求的服务名 + 版本
     */
    private String serviceName;

    /**
     * 请求调用的方法名称
     */
    private String method;

    /**
     * 参数类型
     */
    private Class<?>[] parameterTypes;

    /**
     * 参数
     */
    private Object[] parameterValues;

    /**
     * 一致性哈希参与计算的参数下标。
     *
     * 默认使用第 0 个参数。比如 getUser(userId) 会按 userId 稳定路由；
     * 如果方法是 getOrder(type, orderId)，可以配置为 1，让 orderId 参与路由。
     */
    private int[] hashArguments;

}
