package com.wxy.rpc.server.spring;

import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.annotation.ClassPathBeanDefinitionScanner;
import org.springframework.core.type.filter.AnnotationTypeFilter;

import java.lang.annotation.Annotation;

/**
 * RPC 服务类路径扫描器。
 * 继承了 Spring 的 ClassPathBeanDefinitionScanner，用来复用 Spring 原生的包扫描能力。
 * 它和普通 Spring 扫描器的区别是：指定一个注解类型，例如 @RpcService，然后只扫描并注册带有该注解的类。
 *
 * 在本项目中，它主要由 RpcBeanDefinitionRegistrar 创建，用来扫描 @RpcComponentScan
 * 指定包路径下的 @RpcService 服务实现类，并把这些类注册成 Spring BeanDefinition。
 */
public class RpcClassPathBeanDefinitionScanner extends ClassPathBeanDefinitionScanner {
    // 当前扫描器要识别的注解类型。例如传入 RpcService.class 时，扫描器只会放行带 @RpcService 的类。
    private Class<? extends Annotation> annotationType;

    /**
     * 创建不指定注解类型的扫描器。这种情况下 registerFilters 会注册一个永远返回 true 的过滤器，表示扫描到的类都可以被注册。
     * @param registry Spring BeanDefinition 注册器
     */
    public RpcClassPathBeanDefinitionScanner(BeanDefinitionRegistry registry) {
        super(registry);
        registerFilters();
    }

    /**
     * 创建指定注解类型的扫描器。本项目中主要使用这个构造方法，传入 RpcService.class，表示只扫描 @RpcService 标注的 RPC 服务实现类。
     * @param registry Spring BeanDefinition 注册器
     * @param annotationType 需要被扫描识别的注解类型
     */
    public RpcClassPathBeanDefinitionScanner(BeanDefinitionRegistry registry, Class<? extends Annotation> annotationType) {
        super(registry);
        this.annotationType = annotationType;
        registerFilters();        // 根据 annotationType 注册扫描过滤器。
    }

    /**
     * 注册扫描过滤器，用来决定哪些类可以被扫描并注册成 BeanDefinition。
     *
     * 如果指定了 annotationType，则只放行带有该注解的类。
     * 如果没有指定 annotationType，则放行所有类。
     */
    private void registerFilters() {
        // 放行指定 annotation 类型，例如只放行 @RpcService 标注的类。
        if (annotationType != null) {
            this.addIncludeFilter(new AnnotationTypeFilter(this.annotationType));
        } else {
            // 没有指定注解类型时，所有扫描到的类都放行。
            this.addIncludeFilter((metadataReader, metadataReaderFactory) -> true);
        }
    }

    /**
     * 扫描指定包路径下的类，并把符合过滤条件的类注册成 Spring BeanDefinition。
     * 这里直接调用父类的 scan 方法。父类会完成 classpath 扫描、过滤匹配、BeanDefinition 创建和注册等流程。
     * @param basePackages 需要扫描的包路径
     * @return 成功注册的 BeanDefinition 数量
     */
    @Override
    public int scan(String... basePackages) {
        return super.scan(basePackages);
    }
}
