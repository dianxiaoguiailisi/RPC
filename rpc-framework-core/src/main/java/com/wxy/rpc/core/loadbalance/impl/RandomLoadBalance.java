package com.wxy.rpc.core.loadbalance.impl;
import com.wxy.rpc.core.common.RpcRequest;
import com.wxy.rpc.core.common.ServiceInfo;
import com.wxy.rpc.core.loadbalance.AbstractLoadBalance;
import java.util.List;
import java.util.Random;

/**
 * 随机负载均衡策略实现类
 */
public class RandomLoadBalance extends AbstractLoadBalance {

    final Random random = new Random();

    /** 
     * @param invokers 当前服务可用的 provider 列表
     * @param request
     * @return ServiceInfo
     */
    @Override
    protected ServiceInfo doSelect(List<ServiceInfo> invokers, RpcRequest request) {
        return invokers.get(random.nextInt(invokers.size()));
    }
}
