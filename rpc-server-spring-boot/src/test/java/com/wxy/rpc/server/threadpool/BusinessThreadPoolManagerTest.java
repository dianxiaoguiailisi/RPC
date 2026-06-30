package com.wxy.rpc.server.threadpool;

import com.wxy.rpc.server.config.RpcServerProperties;
import junit.framework.TestCase;

import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 服务端业务线程池管理器测试。
 */
public class BusinessThreadPoolManagerTest extends TestCase {

    public void testDefaultBusinessThreadPoolIsUsedWhenIsolationDisabled() throws Exception {
        RpcServerProperties properties = defaultProperties();
        properties.setBizIsolationEnabled(false);

        BusinessThreadPoolManager.init(properties);

        ThreadPoolExecutor defaultExecutor = BusinessThreadPoolManager.selectExecutor(null);
        ThreadPoolExecutor serviceExecutor = BusinessThreadPoolManager.selectExecutor("demoService-1.0");
        assertSame(defaultExecutor, serviceExecutor);
        BusinessThreadPoolManager.shutdown();
    }

    public void testServiceLevelBusinessThreadPoolIsolation() throws Exception {
        RpcServerProperties properties = defaultProperties();
        properties.setBizIsolationEnabled(true);

        RpcServerProperties.ServiceThreadPoolProperties serviceThreadPoolProperties =
                new RpcServerProperties.ServiceThreadPoolProperties();
        serviceThreadPoolProperties.setServiceName("demoService-1.0");
        serviceThreadPoolProperties.setCorePoolSize(1);
        serviceThreadPoolProperties.setMaxPoolSize(1);
        serviceThreadPoolProperties.setQueueCapacity(1);
        serviceThreadPoolProperties.setKeepAliveSeconds(60L);
        properties.setBizIsolationServices(Collections.singletonList(serviceThreadPoolProperties));

        BusinessThreadPoolManager.init(properties);

        ThreadPoolExecutor defaultExecutor = BusinessThreadPoolManager.selectExecutor("otherService-1.0");
        ThreadPoolExecutor isolatedExecutor = BusinessThreadPoolManager.selectExecutor("demoService-1.0");
        assertNotSame(defaultExecutor, isolatedExecutor);
        assertEquals(1, isolatedExecutor.getCorePoolSize());
        assertEquals(1, isolatedExecutor.getMaximumPoolSize());
        BusinessThreadPoolManager.shutdown();
    }

    private RpcServerProperties defaultProperties() throws Exception {
        RpcServerProperties properties = new RpcServerProperties();
        properties.setBizCorePoolSize(2);
        properties.setBizMaxPoolSize(2);
        properties.setBizQueueCapacity(10);
        properties.setBizKeepAliveSeconds(60L);
        return properties;
    }
}
