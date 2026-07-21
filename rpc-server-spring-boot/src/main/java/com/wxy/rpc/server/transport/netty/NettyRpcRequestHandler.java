package com.wxy.rpc.server.transport.netty;

import com.wxy.rpc.core.common.RpcRequest;
import com.wxy.rpc.core.common.RpcResponse;
import com.wxy.rpc.core.enums.MessageStatus;
import com.wxy.rpc.core.enums.MessageType;
import com.wxy.rpc.core.exception.RpcException;
import com.wxy.rpc.core.factory.SingletonFactory;
import com.wxy.rpc.core.metrics.RpcMetricNames;
import com.wxy.rpc.core.metrics.RpcMetricsCollector;
import com.wxy.rpc.core.metrics.RpcMetricsContext;
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
 * 基于 Netty 的 RPC 服务端入站处理器。
 *
 * <p>上游的 {@code RpcFrameDecoder} 和 {@code SharableRpcMessageCodec}
 * 会先将字节流拆分、解码为完整的 {@link RpcMessage}，然后交给当前处理器。
 * 当前处理器主要负责：</p>
 * <ol>
 *     <li>将 RPC 请求提交到业务线程池，避免阻塞 Netty EventLoop。</li>
 *     <li>调用本地服务，并将返回值或异常封装成 RPC 响应。</li>
 *     <li>处理心跳请求，向客户端返回服务端负载指标。</li>
 *     <li>处理业务线程池拒绝、读空闲以及网络异常。</li>
 * </ol>
 *那么当 Pipeline 中传递的消息类型是 RpcMessage 时，就会调用这个方法。
 */
@Slf4j
public class NettyRpcRequestHandler extends SimpleChannelInboundHandler<RpcMessage> {

    /**
     * RPC 本地调用处理器，负责根据服务名查找本地服务并反射调用目标方法。
     */
    private final RpcRequestHandler rpcRequestHandler;

    public NettyRpcRequestHandler() {
        this.rpcRequestHandler = SingletonFactory.getInstance(RpcRequestHandler.class);
    }

    /**
     * 处理经过上游编解码器还原后的 RPC 消息。
     *
     * <p>该方法运行在 Netty EventLoop 线程中，因此只负责解析消息类型、
     * 选择业务线程池并提交任务，不在 I/O 线程中执行耗时的业务调用。</p>
     *
     * @param ctx 当前 Channel 的处理器上下文
     * @param msg 已解码的 RPC 协议消息
     */
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RpcMessage msg) throws Exception {
        long handlerStart = RpcMetricsCollector.now();//记录 Handler 开始执行时间
        MessageHeader header = msg.getHeader();//获取 RPC 消息头
        MessageType type = MessageType.parseByType(header.getMessageType());//获得消息类型
        RpcRequest request = type == MessageType.REQUEST ? (RpcRequest) msg.getBody() : null;
        // 按服务名选择隔离线程池；心跳等非普通 RPC 消息使用默认线程池。
        ThreadPoolExecutor executor = BusinessThreadPoolManager.selectExecutor(request == null ? null : request.getServiceName());
        //进入任务提交逻辑
        try {
            if (request != null) {
                RpcMetricsCollector.recordSince("server", request.getServiceName(), request.getMethod(), RpcMetricNames.SERVER_HANDLER_COST, handlerStart);
            }
            long enqueueTime = RpcMetricsCollector.now();
            // 将业务调用从 Netty I/O 线程切换到独立的业务线程池。
            executor.submit(() -> handleMessage(ctx, msg, type, request, enqueueTime));
        } catch (RejectedExecutionException e) {
            log.warn("RPC business thread pool rejected request, serviceName: {}, method: {}.",request == null ? "" : request.getServiceName(), request == null ? "" : request.getMethod());
            writeRejectedResponse(ctx, msg, type, request);
            ReferenceCountUtil.release(msg);
        }
    }

    /**
     * 在业务线程中处理 RPC 请求或心跳请求。
     *
     * @param ctx         Channel 处理器上下文
     * @param msg         完整的 RPC 消息
     * @param type        协议头中的消息类型
     * @param request     RPC 请求体；非普通 RPC 请求时为 null
     * @param enqueueTime 任务提交时间，用于统计队列等待耗时
     */
    private void handleMessage(ChannelHandlerContext ctx, RpcMessage msg, MessageType type, RpcRequest request,long enqueueTime) {
        try {
            if (request != null) {
                RpcMetricsCollector.recordSince("server", request.getServiceName(), request.getMethod(),RpcMetricNames.SERVER_QUEUE_COST, enqueueTime);
            }
            MessageHeader header = msg.getHeader();
            log.debug("The message received by the server is: {}", msg.getBody());
            // 如果是心跳检测请求信息
            if (type == MessageType.HEARTBEAT_REQUEST) {
                header.setMessageType(MessageType.HEARTBEAT_RESPONSE.getType());
                header.setMessageStatus(MessageStatus.SUCCESS.getCode());
                RpcMessage responseRpcMessage = buildResponseMessage(header,ServerLoadMetricsCollector.collect(BusinessThreadPoolManager.getExecutors()));
                writeResponse(ctx, responseRpcMessage);
            } else { // 处理 Rpc 请求信息
                // 设置头部消息类型
                header.setMessageType(MessageType.RESPONSE.getType());
                // 查找本地服务并反射调用目标方法。
                try {
                    // 获取本地反射调用结果
                    Object result = rpcRequestHandler.handleRpcRequest(request);
                    // 异步服务不在当前线程阻塞等待，完成后再通过回调写回结果。
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

    /**
     * 为异步服务返回值注册完成回调。
     * 回调最终切换到当前 Channel 的 EventLoop 中写回响应，
     * 避免多个业务线程直接操作 Channel 处理链。
     */
    private void writeAsyncRpcResponse(ChannelHandlerContext ctx, MessageHeader header, RpcRequest request,CompletionStage<?> completionStage) {
        //注册异步完成回调
        completionStage.whenComplete((result, throwable) -> ctx.executor().execute(() -> {
            if (throwable == null) {
                writeRpcResponse(ctx, header, result, null);
                return;
            }
            Throwable cause = unwrap(throwable);
            log.error("The async service [{}], the method [{}] invoke failed!",request.getServiceName(), request.getMethod(), cause);
            writeRpcResponse(ctx, header, null,new RpcException("Error in async remote procedure call, " + cause.getMessage()));
        }));
    }

    /**
     * 根据服务调用结果构造 {@link RpcResponse}。成功时写入返回值，失败时写入异常并设置失败状态。
     */
    private void writeRpcResponse(ChannelHandlerContext ctx, MessageHeader header, Object result, Exception exception) {
        long responseBuildStart = RpcMetricsCollector.now();
        RpcResponse response = new RpcResponse();//创建响应体
        if (exception == null) {//正常响应
            response.setReturnValue(result);
            header.setMessageStatus(MessageStatus.SUCCESS.getCode());//设置消息状态
        } else {
            // 若不设置，堆栈信息过多，导致报错
            response.setExceptionValue(exception);
            header.setMessageStatus(MessageStatus.FAIL.getCode());
        }
        //获取本次调用的指标上下文，通过消息序列号获取当前 RPC 调用对应的监控信息
        RpcMetricsContext.InvocationInfo info = RpcMetricsContext.get(header.getSequenceId());
        //记录响应对象构建耗时
        RpcMetricsCollector.recordSince("server", info, RpcMetricNames.SERVER_RESPONSE_BUILD_COST,responseBuildStart);
        //构建完整消息并写回客户端
        writeResponse(ctx, buildResponseMessage(header, response));
    }

    /**
     * 将协议头和响应体组装为完整的 RPC 消息。
     */
    private RpcMessage buildResponseMessage(MessageHeader header, Object body) {
        RpcMessage responseRpcMessage = new RpcMessage();
        responseRpcMessage.setHeader(header);
        responseRpcMessage.setBody(body);
        return responseRpcMessage;
    }

    /**
     * 将响应沿出站方向写入 Pipeline，后续由协议编码器转换为字节流。
     * 写入完成后清理当次调用的指标上下文，发送失败时关闭 Channel。
     */
    private void writeResponse(ChannelHandlerContext ctx, RpcMessage responseRpcMessage) {
        log.debug("responseRpcMessage: {}.", responseRpcMessage);
        // 将结果写入，传递到下一个处理器
        long writeStart = RpcMetricsCollector.now();
        //写入并立即刷新，注册写完成监听器
        ctx.writeAndFlush(responseRpcMessage).addListener((ChannelFutureListener) future -> {
            //删除本次调用的指标上下文
            RpcMetricsContext.InvocationInfo info = RpcMetricsContext.remove(responseRpcMessage.getHeader().getSequenceId());
            //记录响应写出耗时
            RpcMetricsCollector.recordSince("server", info, RpcMetricNames.SERVER_WRITE_COST, writeStart);
            if (!future.isSuccess()) {
                ChannelFutureListener.CLOSE_ON_FAILURE.operationComplete(future);
            }
        });
    }

    private Throwable unwrap(Throwable throwable) {
        Throwable current = throwable;
        if ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    /**
     * 当业务线程池拒绝任务时，立即构造失败响应，避免客户端一直等待。
     */
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
     * 处理 Netty 用户自定义事件。
     *
     * <p>当 {@code IdleStateHandler} 检测到服务端长时间没有读到客户端的
     * 业务数据或心跳数据时，会产生 {@link IdleState#READER_IDLE} 事件，
     * 当前处理器会关闭该客户端 Channel，清理失效连接。</p>
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

    /**
     * 处理 Pipeline 中的未捕获异常，记录日志并关闭当前连接。
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("server catch exception");
        cause.printStackTrace();
        ctx.close();
    }

}
