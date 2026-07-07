package com.wxy.rpc.client.future;

import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * RPC Promise。
 *
 * 这个类在 CompletableFuture 外面补了一层 Netty 风格的回调 API：
 * 1. addListener：注册完成监听器。
 * 2. setSuccess：设置成功结果。
 * 3. setFailure：设置失败异常。
 *
 * 它本身仍然是 CompletableFuture，所以原来的 thenApply、whenComplete、get 等能力都可以继续使用。
 *
 * @param <T> 响应结果类型
 */
@Slf4j
public class RpcPromise<T> extends CompletableFuture<T> {

    /**
     * 完成监听器列表。
     */
    private final List<RpcFutureListener<T>> listeners = new CopyOnWriteArrayList<>();

    /**
     * 失败原因。
     */
    private volatile Throwable cause;

    /**
     * 请求超时截止时间，单位毫秒。
     *
     * 小于等于 0 表示当前 Promise 不启用客户端请求超时扫描。
     */
    private volatile long timeoutDeadlineMillis = -1L;

    /**
     * 设置请求超时截止时间。
     *
     * @param timeoutDeadlineMillis 截止时间戳，单位毫秒
     */
    public void setTimeoutDeadlineMillis(long timeoutDeadlineMillis) {
        this.timeoutDeadlineMillis = timeoutDeadlineMillis;
    }

    /**
     * 判断当前 Promise 是否已经超时。
     *
     * @param nowMillis 当前时间戳，单位毫秒
     * @return true 表示已超时
     */
    public boolean isTimeout(long nowMillis) {
        return timeoutDeadlineMillis > 0 && nowMillis >= timeoutDeadlineMillis && !isDone();
    }

    /**
     * 添加完成监听器。
     *
     * 如果 Promise 已经完成，立即触发该监听器。
     *
     * @param listener 完成监听器
     * @return 当前 Promise
     */
    public RpcPromise<T> addListener(RpcFutureListener<T> listener) {
        Objects.requireNonNull(listener, "listener");
        if (isDone()) {
            notifyListener(listener);
            return this;
        }
        listeners.add(listener);
        if (isDone() && listeners.remove(listener)) {
            notifyListener(listener);
        }
        return this;
    }

    /**
     * 设置成功结果。
     *
     * @param value 响应结果
     * @return true 表示本次成功完成 Promise
     */
    public boolean setSuccess(T value) {
        return complete(value);
    }

    /**
     * 设置失败结果。
     *
     * @param throwable 失败原因
     * @return true 表示本次成功完成 Promise
     */
    public boolean setFailure(Throwable throwable) {
        return completeExceptionally(throwable);
    }

    /**
     * 判断 Promise 是否成功完成。
     *
     * @return true 表示成功完成
     */
    public boolean isSuccess() {
        return isDone() && !isCompletedExceptionally() && !isCancelled();
    }

    /**
     * 获取失败原因。
     *
     * @return 失败原因
     */
    public Throwable cause() {
        return cause;
    }

    @Override
    public boolean complete(T value) {
        boolean completed = super.complete(value);
        if (completed) {
            notifyListeners();
        }
        return completed;
    }

    @Override
    public boolean completeExceptionally(Throwable ex) {
        boolean completed = super.completeExceptionally(ex);
        if (completed) {
            cause = ex;
            notifyListeners();
        }
        return completed;
    }

    private void notifyListeners() {
        for (RpcFutureListener<T> listener : listeners) {
            notifyListener(listener);
        }
        listeners.clear();
    }

    private void notifyListener(RpcFutureListener<T> listener) {
        try {
            listener.operationComplete(this);
        } catch (Exception e) {
            log.warn("Rpc promise listener execute failed.", e);
        }
    }
}
