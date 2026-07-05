package com.wxy.rpc.server.transport.netty;

import com.wxy.rpc.core.common.RpcRequest;
import com.wxy.rpc.core.common.RpcResponse;
import com.wxy.rpc.core.enums.MessageStatus;
import com.wxy.rpc.core.enums.MessageType;
import com.wxy.rpc.core.exception.RpcException;
import com.wxy.rpc.core.factory.SingletonFactory;
import com.wxy.rpc.core.protocol.MessageHeader;
import com.wxy.rpc.core.protocol.RpcMessage;
import com.wxy.rpc.server.handler.RpcRequestHandler;
import com.wxy.rpc.server.metrics.ServerLoadMetricsCollector;
import com.wxy.rpc.server.threadpool.BusinessThreadPoolManager;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 基于 Netty 的 Rpc 请求消息处理器
 *
 * @author Wuxy
 * @version 1.0
 * @ClassName RpcRequestHandler
 * @Date 2023/1/6 19:42
 */
@Slf4j
public class NettyRpcRequestHandler extends SimpleChannelInboundHandler<RpcMessage> {

    private final RpcRequestHandler rpcRequestHandler;

    public NettyRpcRequestHandler() {
        this.rpcRequestHandler = SingletonFactory.getInstance(RpcRequestHandler.class);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RpcMessage msg) throws Exception {
        MessageHeader header = msg.getHeader();
        MessageType type = MessageType.parseByType(header.getMessageType());
        RpcRequest request = type == MessageType.REQUEST ? (RpcRequest) msg.getBody() : null;
        ThreadPoolExecutor executor = BusinessThreadPoolManager.selectExecutor(
                request == null ? null : request.getServiceName());
        try {
            executor.submit(() -> handleMessage(ctx, msg, type, request));
        } catch (RejectedExecutionException e) {
            log.warn("RPC business thread pool rejected request, serviceName: {}, method: {}.",
                    request == null ? "" : request.getServiceName(), request == null ? "" : request.getMethod());
            writeRejectedResponse(ctx, msg, type, request);
            ReferenceCountUtil.release(msg);
        }
    }

    private void handleMessage(ChannelHandlerContext ctx, RpcMessage msg, MessageType type, RpcRequest request) {
        try {
            MessageHeader header = msg.getHeader();
            log.debug("The message received by the server is: {}", msg.getBody());
            // 如果是心跳检测请求信息
            if (type == MessageType.HEARTBEAT_REQUEST) {
                header.setMessageType(MessageType.HEARTBEAT_RESPONSE.getType());
                header.setMessageStatus(MessageStatus.SUCCESS.getCode());
                RpcMessage responseRpcMessage = buildResponseMessage(header,
                        ServerLoadMetricsCollector.collect(BusinessThreadPoolManager.getExecutors()));
                writeResponse(ctx, responseRpcMessage);
            } else { // 处理 Rpc 请求信息
                // 设置头部消息类型
                header.setMessageType(MessageType.RESPONSE.getType());
                // 反射调用
                try {
                    // 获取本地反射调用结果
                    Object result = rpcRequestHandler.handleRpcRequest(request);
                    if (result instanceof CompletionStage) {
                        writeAsyncRpcResponse(ctx, header, request, (CompletionStage<?>) result);
                        return;
                    }
                    writeRpcResponse(ctx, header, result, null);
                } catch (Exception e) {
                    log.error("The service [{}], the method [{}] invoke failed!", request.getServiceName(), request.getMethod());
                    writeRpcResponse(ctx, header, null, new RpcException("Error in remote procedure call, " + e.getMessage()));
                }
            }
        } finally {
            // 确保 ByteBuf 被释放，防止发生内存泄露
            ReferenceCountUtil.release(msg);
        }
    }

    private void writeAsyncRpcResponse(ChannelHandlerContext ctx, MessageHeader header, RpcRequest request,
                                       CompletionStage<?> completionStage) {
        completionStage.whenComplete((result, throwable) -> ctx.executor().execute(() -> {
            if (throwable == null) {
                writeRpcResponse(ctx, header, result, null);
                return;
            }
            Throwable cause = unwrap(throwable);
            log.error("The async service [{}], the method [{}] invoke failed!",
                    request.getServiceName(), request.getMethod(), cause);
            writeRpcResponse(ctx, header, null,
                    new RpcException("Error in async remote procedure call, " + cause.getMessage()));
        }));
    }

    private void writeRpcResponse(ChannelHandlerContext ctx, MessageHeader header, Object result, Exception exception) {
        RpcResponse response = new RpcResponse();
        if (exception == null) {
            response.setReturnValue(result);
            header.setMessageStatus(MessageStatus.SUCCESS.getCode());
        } else {
            // 若不设置，堆栈信息过多，导致报错
            response.setExceptionValue(exception);
            header.setMessageStatus(MessageStatus.FAIL.getCode());
        }
        writeResponse(ctx, buildResponseMessage(header, response));
    }

    private RpcMessage buildResponseMessage(MessageHeader header, Object body) {
        RpcMessage responseRpcMessage = new RpcMessage();
        responseRpcMessage.setHeader(header);
        responseRpcMessage.setBody(body);
        return responseRpcMessage;
    }

    private void writeResponse(ChannelHandlerContext ctx, RpcMessage responseRpcMessage) {
        log.debug("responseRpcMessage: {}.", responseRpcMessage);
        // 将结果写入，传递到下一个处理器
        ctx.writeAndFlush(responseRpcMessage).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
    }

    private Throwable unwrap(Throwable throwable) {
        Throwable current = throwable;
        if ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private void writeRejectedResponse(ChannelHandlerContext ctx, RpcMessage msg, MessageType type, RpcRequest request) {
        RpcMessage responseRpcMessage = new RpcMessage();
        MessageHeader header = msg.getHeader();
        if (type == MessageType.HEARTBEAT_REQUEST) {
            header.setMessageType(MessageType.HEARTBEAT_RESPONSE.getType());
            header.setMessageStatus(MessageStatus.FAIL.getCode());
            responseRpcMessage.setHeader(header);
            responseRpcMessage.setBody(ServerLoadMetricsCollector.collect(BusinessThreadPoolManager.getExecutors()));
        } else {
            RpcResponse response = new RpcResponse();
            header.setMessageType(MessageType.RESPONSE.getType());
            header.setMessageStatus(MessageStatus.FAIL.getCode());
            String resource = request == null ? "" : request.getServiceName() + "#" + request.getMethod();
            response.setExceptionValue(new RpcException(String.format(
                    "The service method [%s] request was rejected by business thread pool.", resource)));
            responseRpcMessage.setHeader(header);
            responseRpcMessage.setBody(response);
        }
        ctx.writeAndFlush(responseRpcMessage).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
    }

    /**
     * 用户自定义事件，当触发读空闲时，自动关闭【客户端channel】连接
     *
     * @param ctx ctx
     * @param evt evt
     * @throws Exception exception
     */
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleState state = ((IdleStateEvent) evt).state();
            if (state == IdleState.READER_IDLE) {
                log.warn("idle check happen, so close the connection.");
                ctx.close();
            }
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("server catch exception");
        cause.printStackTrace();
        ctx.close();
    }

}
