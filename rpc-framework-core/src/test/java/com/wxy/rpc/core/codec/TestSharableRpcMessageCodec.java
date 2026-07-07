package com.wxy.rpc.core.codec;

import com.wxy.rpc.core.common.RpcRequest;
import com.wxy.rpc.core.enums.SerializationType;
import com.wxy.rpc.core.protocol.MessageHeader;
import com.wxy.rpc.core.protocol.RpcMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import junit.framework.TestCase;

/**
 * @author Wuxy
 * @version 1.0
 * @ClassName TestSharableRpcMessageCodec
 * @Date 2023/1/5 17:19
 */
public class TestSharableRpcMessageCodec extends TestCase {

    public void testShouldEncodeAndDecodeRpcRequestWithAllSerializations() {
        for (SerializationType serializationType : SerializationType.values()) {
            EmbeddedChannel outboundChannel = new EmbeddedChannel(new SharableRpcMessageCodec());
            EmbeddedChannel inboundChannel = new EmbeddedChannel(new RpcFrameDecoder(), new SharableRpcMessageCodec());

            RpcMessage rpcMessage = new RpcMessage();
            rpcMessage.setHeader(MessageHeader.build(serializationType.name()));
            rpcMessage.setBody(buildRequest());

            assertTrue(outboundChannel.writeOutbound(rpcMessage));
            ByteBuf encoded = outboundChannel.readOutbound();

            assertTrue(inboundChannel.writeInbound(encoded));
            RpcMessage decoded = inboundChannel.readInbound();
            RpcRequest decodedRequest = (RpcRequest) decoded.getBody();

            assertEquals("com.wxy.rpc.api.service.HelloService-1.0", decodedRequest.getServiceName());
            assertEquals("sayHello", decodedRequest.getMethod());
            assertEquals(String.class, decodedRequest.getParameterTypes()[0]);
            assertEquals("zhangsan", decodedRequest.getParameterValues()[0]);

            outboundChannel.finishAndReleaseAll();
            inboundChannel.finishAndReleaseAll();
        }
    }

    private RpcRequest buildRequest() {
        RpcRequest request = new RpcRequest();
        request.setServiceName("com.wxy.rpc.api.service.HelloService-1.0");
        request.setMethod("sayHello");
        request.setParameterTypes(new Class[]{String.class});
        request.setParameterValues(new Object[]{"zhangsan"});
        return request;
    }

    static class Server {
        public static void main(String[] args) {
            NioEventLoopGroup boss = new NioEventLoopGroup();
            NioEventLoopGroup worker = new NioEventLoopGroup();
        }
    }

    static class Client {
        public static void main(String[] args) {

        }
    }

}
