package com.wxy.rpc.core.metrics;

/**
 * RPC 链路阶段指标名称。
 */
public final class RpcMetricNames {

    public static final String CLIENT_PROXY_COST = "clientProxyCost";
    public static final String CLIENT_DISCOVERY_COST = "clientDiscoveryCost";
    public static final String CLIENT_LOAD_BALANCE_COST = "clientLoadBalanceCost";
    public static final String CLIENT_ENCODE_COST = "clientEncodeCost";
    public static final String CLIENT_WRITE_COST = "clientWriteCost";
    public static final String CLIENT_WAIT_RESPONSE_COST = "clientWaitResponseCost";
    public static final String CLIENT_DECODE_COST = "clientDecodeCost";
    public static final String CLIENT_PROMISE_COMPLETE_COST = "clientPromiseCompleteCost";

    public static final String SERVER_DECODE_COST = "serverDecodeCost";
    public static final String SERVER_HANDLER_COST = "serverHandlerCost";
    public static final String SERVER_QUEUE_COST = "serverQueueCost";
    public static final String SERVER_RATE_LIMIT_COST = "serverRateLimitCost";
    public static final String SERVER_LOCAL_SERVICE_LOOKUP_COST = "serverLocalServiceLookupCost";
    public static final String SERVER_METHOD_LOOKUP_COST = "serverMethodLookupCost";
    public static final String SERVER_REFLECT_INVOKE_COST = "serverReflectInvokeCost";
    public static final String SERVER_BIZ_COST = "serverBizCost";
    public static final String SERVER_RESPONSE_BUILD_COST = "serverResponseBuildCost";
    public static final String SERVER_ENCODE_COST = "serverEncodeCost";
    public static final String SERVER_WRITE_COST = "serverWriteCost";

    public static final String TOTAL_COST = "totalCost";

    private RpcMetricNames() {
    }
}
