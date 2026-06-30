package com.wxy.rpc.client.proxy;

import com.wxy.rpc.core.common.RpcRequest;
import com.wxy.rpc.core.common.ServiceInfo;
import com.wxy.rpc.core.loadbalance.AbstractLoadBalance;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 客户端自适应负载均衡。
 *
 * 该策略不只依赖静态权重或轮询下标，而是根据客户端记录的 provider 实时状态动态选择节点。
 * 当前综合考虑：
 * 1. activeCount：当前未完成请求数。
 * 2. averageResponseMillis：历史平均响应时间。
 * 3. failureCount：连续失败次数。
 * 4. slowCallCount：连续慢调用次数。
 *
 * 分数越低，说明节点当前越健康、越空闲，越优先被选择。
 */
public class AdaptiveLoadBalance extends AbstractLoadBalance {

    private final Random random = new Random();

    @Override
    protected ServiceInfo doSelect(List<ServiceInfo> invokers, RpcRequest request) {
        long bestScore = Long.MAX_VALUE;
        List<ServiceInfo> bestInvokers = new ArrayList<>();
        for (ServiceInfo invoker : invokers) {
            long score = ProviderHealthManager.score(invoker);
            if (score < bestScore) {
                bestScore = score;
                bestInvokers.clear();
                bestInvokers.add(invoker);
            } else if (score == bestScore) {
                bestInvokers.add(invoker);
            }
        }
        if (bestInvokers.isEmpty()) {
            return null;
        }
        return bestInvokers.get(random.nextInt(bestInvokers.size()));
    }
}
