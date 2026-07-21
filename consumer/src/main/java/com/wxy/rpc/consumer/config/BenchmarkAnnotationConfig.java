package com.wxy.rpc.consumer.config;

import com.wxy.rpc.core.common.RpcRequest;
import com.wxy.rpc.core.common.ServiceInfo;
import com.wxy.rpc.core.discovery.ServiceDiscovery;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.core.io.support.EncodedResource;
import org.springframework.core.io.support.PropertySourceFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * 性能测试配置类
 *
 * @author Wuxy
 * @version 1.0
 * @ClassName BenchmarkAnnotationConfig
 * @since 2023/2/22 16:36
 */
@ComponentScan("com.wxy.rpc")
@Configuration
@PropertySource(value = "classpath:application.yml", factory = BenchmarkAnnotationConfig.YamlPropertySourceFactory.class)
public class BenchmarkAnnotationConfig {

    /**
     * 远程压测可直接指定 provider 地址，避免压测机必须能访问注册中心。
     *
     * 示例：-Drpc.benchmark.providers=119.45.130.240:9991,175.27.163.96:9991
     */
    public static ServiceDiscovery createBenchmarkServiceDiscovery(String providerAddresses) {
        List<ServiceInfo> providers = Arrays.stream(providerAddresses.split(","))
                .map(String::trim)
                .filter(value -> !value.isEmpty())
                .map(BenchmarkAnnotationConfig::parseProvider)
                .collect(Collectors.toList());
        if (providers.isEmpty()) {
            throw new IllegalArgumentException("rpc.benchmark.providers must contain at least one host:port");
        }

        return new ServiceDiscovery() {
            @Override
            public ServiceInfo discover(RpcRequest request) {
                return getServicesUnchecked(request.getServiceName()).get(0);
            }

            @Override
            public List<ServiceInfo> getServices(String serviceName) {
                return getServicesUnchecked(serviceName);
            }

            private List<ServiceInfo> getServicesUnchecked(String serviceName) {
                return providers.stream()
                        .map(provider -> ServiceInfo.builder()
                                .appName(provider.getAppName())
                                .serviceName(serviceName)
                                .version("")
                                .address(provider.getAddress())
                                .port(provider.getPort())
                                .build())
                        .collect(Collectors.toList());
            }

            @Override
            public void destroy() {
                // Static provider addresses do not hold registry resources.
            }
        };
    }

    private static ServiceInfo parseProvider(String value) {
        int separator = value.lastIndexOf(':');
        if (separator <= 0 || separator == value.length() - 1) {
            throw new IllegalArgumentException("Invalid provider address: " + value);
        }
        return ServiceInfo.builder()
                .appName("benchmark-provider")
                .address(value.substring(0, separator))
                .port(Integer.parseInt(value.substring(separator + 1)))
                .build();
    }

    /**
     * 读取 yaml 配置文件的属性工厂类
     */
    static class YamlPropertySourceFactory implements PropertySourceFactory {

        @Override
        public org.springframework.core.env.PropertySource<?> createPropertySource(String name, EncodedResource resource) throws IOException {
            Properties propertiesFromYaml = loadYamlIntoProperties(resource);
            String sourceName = name != null ? name : resource.getResource().getFilename();
            return new PropertiesPropertySource(Objects.requireNonNull(sourceName), propertiesFromYaml);
        }

        private Properties loadYamlIntoProperties(EncodedResource resource) throws FileNotFoundException {
            try {
                YamlPropertiesFactoryBean factory = new YamlPropertiesFactoryBean();
                factory.setResources(resource.getResource());
                factory.afterPropertiesSet();
                return factory.getObject();
            } catch (IllegalStateException e) {
                // for ignoreResourceNotFound
                Throwable cause = e.getCause();
                if (cause instanceof FileNotFoundException)
                    throw (FileNotFoundException) e.getCause();
                throw e;
            }
        }
    }
}
