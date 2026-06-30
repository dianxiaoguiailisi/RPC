package com.wxy.rpc.server.spring;

import com.wxy.rpc.server.annotation.RpcComponentScan;
import com.wxy.rpc.server.annotation.RpcService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.ResourceLoaderAware;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.core.type.StandardAnnotationMetadata;

/**
 * RPC 服务 BeanDefinition 注册器。
 *
 * 这个类是 @RpcComponentScan 注解的执行入口。因为 @RpcComponentScan 通过 @Import 导入了当前类，
 * 而当前类实现了 ImportBeanDefinitionRegistrar，所以 Spring 在解析启动类上的 @RpcComponentScan 时，
 * 会回调 registerBeanDefinitions 方法。
 *
 * 它的职责只做一件事：读取 @RpcComponentScan 配置的扫描包路径，
 * 扫描这些包下所有带 @RpcService 注解的类，并把它们注册成 Spring BeanDefinition。
 * 后续服务注册到注册中心、加入本地服务缓存、启动 RPC Server 等动作，由
 * RpcServerBeanPostProcessor 继续完成。
 */
@Slf4j
public class RpcBeanDefinitionRegistrar implements ImportBeanDefinitionRegistrar, ResourceLoaderAware {

    //Spring 资源加载器，用于让自定义扫描器读取 classpath 下的 class 资源。
    private ResourceLoader resourceLoader;

    @Override
    public void setResourceLoader(ResourceLoader resourceLoader) {
        this.resourceLoader = resourceLoader;
    }

    /**
     * 注册 RPC 服务 BeanDefinition。
     *
     * 执行流程：
     * <ol>
     *     <li>从启动类元信息中读取 @RpcComponentScan 的属性。</li>
     *     <li>获取 basePackages，也就是需要扫描的 RPC 服务包路径。</li>
     *     <li>如果没有显式配置扫描包，则默认扫描启动类所在包。</li>
     *     <li>创建只识别 @RpcService 注解的类路径扫描器。</li>
     *     <li>扫描目标包，并将扫描到的 RPC 服务实现类注册为 Spring BeanDefinition。</li>
     * </ol>
     *
     * @param annotationMetadata 导入当前注册器的类的注解元信息，通常就是 ProviderApplication 的元信息
     * @param registry           当前 Spring 容器的 BeanDefinition 注册器
     */
    @Override
    public void registerBeanDefinitions(AnnotationMetadata annotationMetadata, BeanDefinitionRegistry registry) {
        // 读取 @RpcComponentScan 注解中的属性，例如 basePackages/value。
        AnnotationAttributes annotationAttributes = AnnotationAttributes
                .fromMap(annotationMetadata.getAnnotationAttributes(RpcComponentScan.class.getName()));
        String[] basePackages = {};
        if (annotationAttributes != null) {
            // @RpcComponentScan 的 value 和 basePackages 互为别名，这里统一读取 basePackages。
            basePackages = annotationAttributes.getStringArray("basePackages");
        }
        // 如果没有指定扫描包，则默认扫描使用 @RpcComponentScan 的启动类所在包。
        if (basePackages.length == 0) {
            basePackages = new String[]{((StandardAnnotationMetadata) annotationMetadata).getIntrospectedClass().getPackage().getName()};
        }
        // 创建一个只扫描 @RpcService 注解的 Scanner。
        RpcClassPathBeanDefinitionScanner rpcServiceScanner = new RpcClassPathBeanDefinitionScanner(registry, RpcService.class);

        if (this.resourceLoader != null) {
            rpcServiceScanner.setResourceLoader(this.resourceLoader);
        }

        // 扫描包下的所有 RPC 服务类，并返回注册成功的数量。
        // scan 方法会把扫描到的类转换成 BeanDefinition 并注册到 Spring 容器。
        int count = rpcServiceScanner.scan(basePackages);
        log.info("The number of BeanDefinition scanned and registered by RpcServiceScanner is {}.", count);
    }
}
