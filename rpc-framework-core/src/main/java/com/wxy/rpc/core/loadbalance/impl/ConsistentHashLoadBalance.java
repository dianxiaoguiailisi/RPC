package com.wxy.rpc.core.loadbalance.impl;

import com.wxy.rpc.core.common.RpcRequest;
import com.wxy.rpc.core.common.ServiceInfo;
import com.wxy.rpc.core.loadbalance.AbstractLoadBalance;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 一致性哈希负载均衡算法 <p>
 */
public class ConsistentHashLoadBalance extends AbstractLoadBalance {

    private static final int DEFAULT_REPLICA_NUMBER = 160;

    private static final int[] DEFAULT_HASH_ARGUMENTS = new int[]{0};

    private final Map<String, ConsistentHashSelector> selectors = new ConcurrentHashMap<>();

    /** 
     * @param invokers
     * @param request
     * @return ServiceInfo
     */
    @Override
    public ServiceInfo doSelect(List<ServiceInfo> invokers, RpcRequest request) {
        // 得到请求的方法名称
        String method = request.getMethod();
        int[] hashArguments = getHashArguments(request);
        // 构建对应的 key 值，key = 全限定类名 + "." + 方法名，比如 com.xxx.DemoService.sayHello
        String key = request.getServiceName() + "." + method + "." + Arrays.toString(hashArguments);
        // 根据服务列表内容计算 hash，只有 provider 列表真的变化时才重建哈希环。
        int invokersHashCode = invokers.hashCode();
        ConsistentHashSelector selector = selectors.compute(key, (selectorKey, oldSelector) -> {
            if (oldSelector == null || oldSelector.invokersHashCode != invokersHashCode) {
                return new ConsistentHashSelector(invokers, DEFAULT_REPLICA_NUMBER, invokersHashCode);
            }
            return oldSelector;
        });
        // 调用 ConsistentHashSelector 的 select 方法选择 Invoker
        String selectKey = buildSelectKey(request, hashArguments);
        return selector.select(selectKey);
    }

    private int[] getHashArguments(RpcRequest request) {
        int[] hashArguments = request.getHashArguments();
        if (hashArguments == null || hashArguments.length == 0) {
            return DEFAULT_HASH_ARGUMENTS;
        }
        return hashArguments;
    }

    private String buildSelectKey(RpcRequest request, int[] hashArguments) {
        StringBuilder selectKey = new StringBuilder(request.getServiceName())
                .append(".")
                .append(request.getMethod());
        Object[] parameterValues = request.getParameterValues();
        if (parameterValues == null || parameterValues.length == 0) {
            return selectKey.toString();
        }
        for (int hashArgument : hashArguments) {
            if (hashArgument >= 0 && hashArgument < parameterValues.length) {
                selectKey.append(".").append(String.valueOf(parameterValues[hashArgument]));
            }
        }
        return selectKey.toString();
    }

    private final static class ConsistentHashSelector {

        /**
         * 使用 TreeMap 存储虚拟节点（virtualInvokers 需要提供高效的查询操作，因此选用 TreeMap 作为存储结构）
         */
        private final TreeMap<Long, ServiceInfo> virtualInvokers;

        /**
         * provider 列表的内容哈希。
         */
        private final int invokersHashCode;

        /**
         * 构建一个 ConsistentHashSelector 对象
         *
         * @param invokers         存储虚拟节点
         * @param replicaNumber    虚拟节点数，默认为 160
         * @param invokersHashCode provider 列表的内容哈希
         */
        public ConsistentHashSelector(List<ServiceInfo> invokers, int replicaNumber, int invokersHashCode) {
            this.virtualInvokers = new TreeMap<>();
            this.invokersHashCode = invokersHashCode;

            for (ServiceInfo invoker : invokers) {
                String address = invoker.getAddress() + ":" + invoker.getPort();
                for (int i = 0; i < replicaNumber / 4; i++) {
                    // 对 address + i 进行 md5 运算，得到一个长度为16的字节数组
                    byte[] digest = md5(address + i);
                    // 对 digest 部分字节进行4次 hash 运算，得到四个不同的 long 型正整数
                    for (int h = 0; h < 4; h++) {
                        // h = 0 时，取 digest 中下标为 0 ~ 3 的4个字节进行位运算
                        // h = 1 时，取 digest 中下标为 4 ~ 7 的4个字节进行位运算
                        // h = 2, h = 3 时过程同上
                        long m = hash(digest, h);
                        // 将 hash 到 invoker 的映射关系存储到 virtualInvokers 中
                        virtualInvokers.put(m, invoker);
                    }
                }
            }
        }

        /**
         * 进行 md5 运算，返回摘要字节数组
         *
         * @param key 编码字符串 key
         * @return 编码后的摘要内容，长度为 16 的字节数组
         */
        private byte[] md5(String key) {
            MessageDigest md;
            try {
                md = MessageDigest.getInstance("MD5");
                byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
                md.update(bytes);
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
            return md.digest();
        }

        /**
         * 根据摘要生成 hash 值
         *
         * @param digest md5摘要内容
         * @param number 当前索引数
         * @return hash 值
         */
        private long hash(byte[] digest, int number) {
            return (((long) (digest[3 + number * 4] & 0xFF) << 24)
                    | ((long) (digest[2 + number * 4] & 0xFF) << 16)
                    | ((long) (digest[1 + number * 4] & 0xFF) << 8)
                    | (digest[number * 4] & 0xFF))
                    & 0xFFFFFFFFL;
        }

        public ServiceInfo select(String key) {
            // 对参数 key 进行 md5 运算
            byte[] digest = md5(key);
            // 取 digest 数组的前四个字节进行 hash 运算，再将 hash 值传给 selectForKey 方法，
            // 寻找合适的 Invoker
            return selectForKey(hash(digest, 0));
        }

        /**
         * 得到第一个大于等于 hash 值的服务信息，若没有则返回第一个
         *
         * @param hash 哈希值
         * @return 服务信息
         */
        private ServiceInfo selectForKey(long hash) {
            // 找到 TreeMap 中查找第一个节点值大于或等于当前 hash 的 Invoker
            Map.Entry<Long, ServiceInfo> entry = virtualInvokers.ceilingEntry(hash);
            // 如果 hash 大于 Invoker 在圆环上最大的位置，此时 entry = null，需要将 TreeMap 的头节点赋值给 entry
            if (entry == null) {
                entry = virtualInvokers.firstEntry();
            }
            return entry.getValue();
        }
    }
}
