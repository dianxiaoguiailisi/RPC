package com.wxy.rpc.client.future;

/**
 * RPC Future 完成监听器。
 *
 * 作用类似 Netty 的 GenericFutureListener：异步调用完成后触发回调。
 *
 * @param <T> Future 结果类型
 */
@FunctionalInterface
public interface RpcFutureListener<T> {

    /**
     * 异步操作完成后的回调方法。
     *
     * @param promise 已经完成的 Promise
     */
    void operationComplete(RpcPromise<T> promise);
}
