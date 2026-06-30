package com.wxy.rpc.core.common;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

/**
 * 心跳检查消息类：用于 Netty 心跳检测
 */
@Data
@Builder
public class HeartbeatMessage implements Serializable {

    /**
     * 消息
     */
    private String msg;

}
