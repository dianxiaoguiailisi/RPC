package com.wxy.rpc.client.transport.netty;

import com.wxy.rpc.client.common.RequestMetadata;
import com.wxy.rpc.client.future.RpcPromise;
import com.wxy.rpc.client.handler.RpcResponseHandler;
import com.wxy.rpc.client.transport.RpcClient;
import com.wxy.rpc.core.codec.RpcFrameDecoder;
import com.wxy.rpc.core.codec.SharableRpcMessageCodec;
import com.wxy.rpc.core.exception.RpcException;
import com.wxy.rpc.core.factory.SingletonFactory;
import com.wxy.rpc.core.metrics.RpcMetricNames;
import com.wxy.rpc.core.metrics.RpcMetricsCollector;
import com.wxy.rpc.core.metrics.RpcMetricsContext;
import com.wxy.rpc.core.protocol.RpcMessage;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.ScheduledFuture;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 基于 Netty 实现的 Rpc Client 类
 */
@Slf4j
public class NettyRpcClient implements RpcClient {

    /**
     * 请求超时扫描初始延迟，单位毫秒。
     */
    private static final long TIMEOUT_SCAN_INITIAL_DELAY_MILLIS = 100L;

    /**
     * 请求超时扫描间隔，单位毫秒。
     *
     * 这里用一个周期任务统一扫描未完成请求，避免每个 RPC 请求都向 EventLoop 注册一个一次性超时任务。
     */
    private static final long TIMEOUT_SCAN_INTERVAL_MILLIS = 100L;

    /**
     * Netty 通信启动类
     */
    private final Bootstrap bootstrap;

    /**
     * 事件循环对象组，每一个事件循环对象对应一个线程（维护一个 Selector），用来处理 channel 上的 io 事件
     */
    private final EventLoopGroup eventLoopGroup;

    /**
     * Channel 对象缓存工具类
     */
    private final ChannelProvider channelProvider;

    /**
     * 统一请求超时扫描任务。
     */
    private final ScheduledFuture<?> timeoutScannerFuture;

    public NettyRpcClient() {
        bootstrap = new Bootstrap();
        eventLoopGroup = new NioEventLoopGroup();
        bootstrap.group(eventLoopGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        // 超过 15s 内如果没有向服务器写数据，会触发一个 IdleState#WRITE_IDLE 事件
                        ch.pipeline().addLast(new IdleStateHandler(0, 15, 0, TimeUnit.SECONDS));
                        // 添加 粘包拆包 解码器
                        ch.pipeline().addLast(new RpcFrameDecoder());
                        // 添加 协议编解码器
                        ch.pipeline().addLast(new SharableRpcMessageCodec());
                        // 添加 rpc 响应消息处理器
                        ch.pipeline().addLast(new RpcResponseHandler());
                    }
                });
        this.channelProvider = SingletonFactory.getInstance(ChannelProvider.class);
        this.timeoutScannerFuture = eventLoopGroup.next().scheduleAtFixedRate(this::scanTimeoutRequests,
                TIMEOUT_SCAN_INITIAL_DELAY_MILLIS, TIMEOUT_SCAN_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
    }

    @SneakyThrows
    @Override
    public RpcMessage sendRpcRequest(RequestMetadata requestMetadata) {
        RpcPromise<RpcMessage> responseFuture = sendRpcRequestAsync(requestMetadata);
        Integer timeout = requestMetadata.getTimeout();
        try {
            if (timeout == null || timeout <= 0) {
                return responseFuture.get();
            }
            return responseFuture.get(timeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            int sequenceId = requestMetadata.getRpcMessage().getHeader().getSequenceId();
            RpcResponseHandler.UNPROCESSED_RPC_RESPONSES.remove(sequenceId);
            RpcMetricsContext.remove(sequenceId);
            responseFuture.setFailure(e);
            throw new RpcException(String.format("The Remote procedure call exceeded the specified timeout of %dms.",
                    timeout), e);
        } catch (Exception e) {
            throw new RpcException(e);
        }
    }

    /**
     * 异步发送 RPC 请求。
     *
     * Netty 的网络发送本身是异步的，这里用 RpcPromise 承接响应结果。
     * 请求发送前会把 sequenceId 和 Promise 放入未完成请求表，响应回来后由 RpcResponseHandler 完成对应 Promise。
     *
     * @param requestMetadata rpc 请求元数据
     * @return 响应结果 Promise
     */
    @Override
    public RpcPromise<RpcMessage> sendRpcRequestAsync(RequestMetadata requestMetadata) {
        RpcPromise<RpcMessage> promise = new RpcPromise<>();
        // 获取 Channel 对象
        Channel channel = getChannel(new InetSocketAddress(requestMetadata.getServerAddr(), requestMetadata.getPort()));
        if (channel.isActive()) {
            // 获取请求的序列号 ID
            int sequenceId = requestMetadata.getRpcMessage().getHeader().getSequenceId();
            // 存入还未处理的请求
            RpcResponseHandler.UNPROCESSED_RPC_RESPONSES.put(sequenceId, promise);
            Integer timeout = requestMetadata.getTimeout();
            if (timeout != null && timeout > 0) {
                promise.setTimeoutDeadlineMillis(System.currentTimeMillis() + timeout);
            }
            // 发送数据并监听发送状态
            long writeStart = RpcMetricsCollector.now();
            channel.writeAndFlush(requestMetadata.getRpcMessage()).addListener((ChannelFutureListener) future -> {
                RpcMetricsContext.InvocationInfo info = RpcMetricsContext.get(sequenceId);
                RpcMetricsCollector.recordSince("client", info, RpcMetricNames.CLIENT_WRITE_COST, writeStart);
                if (future.isSuccess()) {
                    log.debug("The client send the message successfully, msg: [{}].", requestMetadata);
                } else {
                    future.channel().close();
                    RpcResponseHandler.UNPROCESSED_RPC_RESPONSES.remove(sequenceId);
                    RpcMetricsContext.remove(sequenceId);
                    promise.setFailure(future.cause());
                    log.error("The client send the message failed.", future.cause());
                }
            });
            return promise;
        } else {
            promise.setFailure(new IllegalStateException("The channel is inactivate."));
            return promise;
        }
    }

    /**
     * 扫描并处理已经超时的 RPC 请求。
     *
     * 早期做法是在每个请求发送时调用 EventLoop#schedule 注册一个一次性超时任务。
     * 高并发场景下，请求越多，定时任务对象和调度队列压力越大。这里改成一个固定周期任务扫描未完成请求表：
     * 1. 每个请求只记录自己的超时截止时间。
     * 2. 扫描器发现请求过期后，将 Promise 标记为失败。
     * 3. 再从 UNPROCESSED_RPC_RESPONSES 中移除，避免后续响应误唤醒和内存泄漏。
     */
    private void scanTimeoutRequests() {
        long nowMillis = System.currentTimeMillis();
        for (Map.Entry<Integer, RpcPromise<RpcMessage>> entry :
                RpcResponseHandler.UNPROCESSED_RPC_RESPONSES.entrySet()) {
            Integer sequenceId = entry.getKey();
            RpcPromise<RpcMessage> promise = entry.getValue();
            if (promise.isTimeout(nowMillis)) {
                TimeoutException timeoutException = new TimeoutException(
                        "The Remote procedure call exceeded the specified timeout.");
                if (promise.setFailure(timeoutException)) {
                    RpcResponseHandler.UNPROCESSED_RPC_RESPONSES.remove(sequenceId, promise);
                    RpcMetricsContext.remove(sequenceId);
                }
            }
        }
    }

    /**
     * 连接到服务器获取 channel 对象
     *
     * @param inetSocketAddress 服务端地址
     * @return channel 对象
     */
    @SneakyThrows
    public Channel doConnect(InetSocketAddress inetSocketAddress) {
        // 方式一，不用打印提示信息可以使用的方案
        // sync() 同步等待异步connect连接成功
//        Channel channel = bootstrap.connect(inetSocketAddress).sync().channel();
        // 设置同步等待异步关闭完成
//        channel.closeFuture().sync();

        // 方式二，打印提示信息使用方案
        CompletableFuture<Channel> completableFuture = new CompletableFuture<>();
        bootstrap.connect(inetSocketAddress).addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                log.debug("The client has successfully connected to server [{}]!", inetSocketAddress.toString());
                completableFuture.complete(future.channel());
            } else {
                RpcException exception = new RpcException(String.format("The client failed to connect to [%s].",
                        inetSocketAddress.toString()), future.cause());
                completableFuture.completeExceptionally(exception);
                log.warn("The client failed to connect to server [{}].", inetSocketAddress, future.cause());
            }
        });
        // 等待 future 完成返回结果
        Channel channel = completableFuture.get();
        // 添加异步关闭之后的操作
        channel.closeFuture().addListener(future -> {
            log.info("The client has been disconnected from server [{}].", inetSocketAddress.toString());
        });
        return channel;
    }

    /**
     * 获取 Channel
     *
     * @param inetSocketAddress 通信地址
     * @return 返回对应的 channel 对象
     */
    public Channel getChannel(InetSocketAddress inetSocketAddress) {
        Channel channel = channelProvider.get(inetSocketAddress);
        if (channel == null) {
            channel = doConnect(inetSocketAddress);
            channelProvider.set(inetSocketAddress, channel);
        }
        return channel;
    }

    @Override
    public void close() {
        timeoutScannerFuture.cancel(false);
        channelProvider.closeAll();
        eventLoopGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
    }
}
