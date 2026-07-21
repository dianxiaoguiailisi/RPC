package com.wxy.rpc.core.loadbalance;

import com.wxy.rpc.core.common.RpcRequest;
import com.wxy.rpc.core.common.ServiceInfo;
import com.wxy.rpc.core.loadbalance.impl.ConsistentHashLoadBalance;
import junit.framework.TestCase;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * ConsistentHashLoadBalance unit tests.
 */
public class ConsistentHashLoadBalanceTest extends TestCase {

    public void testSameRequestShouldSelectSameProvider() {
        ConsistentHashLoadBalance loadBalance = new ConsistentHashLoadBalance();
        List<ServiceInfo> providers = providers();
        RpcRequest request = request(new Object[]{"user-1001", "order-1001"}, new int[]{0});

        ServiceInfo first = loadBalance.select(providers, request);
        ServiceInfo second = loadBalance.select(providers, request);

        assertNotNull(first);
        assertSame(first, second);
    }

    public void testDifferentHashArgumentsShouldUseDifferentSelectorCache() throws Exception {
        ConsistentHashLoadBalance loadBalance = new ConsistentHashLoadBalance();
        List<ServiceInfo> providers = providers();

        loadBalance.select(providers, request(new Object[]{"user-1001", "order-1001"}, new int[]{0}));
        loadBalance.select(providers, request(new Object[]{"user-1001", "order-1001"}, new int[]{1}));

        assertEquals(2, getSelectors(loadBalance).size());
    }

    public void testSameProviderContentShouldReuseSelector() throws Exception {
        ConsistentHashLoadBalance loadBalance = new ConsistentHashLoadBalance();
        RpcRequest request = request(new Object[]{"user-1001"}, new int[]{0});
        List<ServiceInfo> providers = providers();

        loadBalance.select(providers, request);
        Object firstSelector = getSelectors(loadBalance).values().iterator().next();

        loadBalance.select(new ArrayList<>(providers), request);
        Object secondSelector = getSelectors(loadBalance).values().iterator().next();

        assertSame(firstSelector, secondSelector);
    }

    public void testProviderContentChangedShouldRebuildSelector() throws Exception {
        ConsistentHashLoadBalance loadBalance = new ConsistentHashLoadBalance();
        RpcRequest request = request(new Object[]{"user-1001"}, new int[]{0});
        List<ServiceInfo> providers = providers();

        loadBalance.select(providers, request);
        Object firstSelector = getSelectors(loadBalance).values().iterator().next();

        List<ServiceInfo> changedProviders = new ArrayList<>(providers);
        changedProviders.add(provider("provider-4", "10.0.0.4", 9994));
        loadBalance.select(changedProviders, request);
        Object secondSelector = getSelectors(loadBalance).values().iterator().next();

        assertNotSame(firstSelector, secondSelector);
    }

    @SuppressWarnings("unchecked")
    private Map<String, ?> getSelectors(ConsistentHashLoadBalance loadBalance) throws Exception {
        Field selectorsField = ConsistentHashLoadBalance.class.getDeclaredField("selectors");
        selectorsField.setAccessible(true);
        return (Map<String, ?>) selectorsField.get(loadBalance);
    }

    private RpcRequest request(Object[] parameterValues, int[] hashArguments) {
        RpcRequest request = new RpcRequest();
        request.setServiceName("com.wxy.rpc.api.service.HelloService-1.0");
        request.setMethod("sayHello");
        request.setParameterValues(parameterValues);
        request.setHashArguments(hashArguments);
        return request;
    }

    private List<ServiceInfo> providers() {
        return Arrays.asList(
                provider("provider-1", "10.0.0.1", 9991),
                provider("provider-2", "10.0.0.2", 9992),
                provider("provider-3", "10.0.0.3", 9993)
        );
    }

    private ServiceInfo provider(String appName, String address, int port) {
        return ServiceInfo.builder()
                .appName(appName)
                .serviceName("com.wxy.rpc.api.service.HelloService-1.0")
                .version("1.0")
                .address(address)
                .port(port)
                .build();
    }
}
