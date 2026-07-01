package com.wxy.rpc.client.transport;

import com.wxy.rpc.client.common.RequestMetadata;
import com.wxy.rpc.client.future.RpcPromise;
import com.wxy.rpc.core.protocol.RpcMessage;

import java.util.concurrent.CompletableFuture;

/**
 * Rpc 客户端类，负责向服务端发起请求（远程过程调用）
 *
 * @author Wuxy
 * @version 1.0
 * @ClassName RpcClient
 * @Date 2023/1/6 17:28
 */
public interface RpcClient {

    /**
     * 发起远程过程调用
     *
     * @param requestMetadata rpc 请求元数据
     * @return 响应结果
     */
    RpcMessage sendRpcRequest(RequestMetadata requestMetadata);

    /**
     * 异步发起远程过程调用。
     *
     * 默认实现使用同步调用包装，Netty 实现会覆盖为真正的异步发送。
     *
     * @param requestMetadata rpc 请求元数据
     * @return 响应结果 Future
     */
    default RpcPromise<RpcMessage> sendRpcRequestAsync(RequestMetadata requestMetadata) {
        RpcPromise<RpcMessage> promise = new RpcPromise<>();
        CompletableFuture.runAsync(() -> {
            try {
                promise.setSuccess(sendRpcRequest(requestMetadata));
            } catch (Throwable throwable) {
                promise.setFailure(throwable);
            }
        });
        return promise;
    }

    /**
     * 关闭客户端占用的网络线程、连接等资源。
     *
     * 默认实现为空，只有 Netty 这类需要维护 EventLoopGroup 的客户端需要覆盖。
     */
    default void close() {
    }

}
