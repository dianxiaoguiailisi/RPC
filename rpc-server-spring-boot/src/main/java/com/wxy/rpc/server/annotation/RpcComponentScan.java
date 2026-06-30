package com.wxy.rpc.server.annotation;

import com.wxy.rpc.server.spring.RpcBeanDefinitionRegistrar;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.annotation.Import;
import org.springframework.core.annotation.AliasFor;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;

import java.lang.annotation.*;

/**
 * 自定义 Rpc 组件扫描注解
 * <p>
 * {@link RpcComponentScan} 类上用 @{@link Import} 引入了 {@link RpcBeanDefinitionRegistrar} 类，而这个类是一个 {@link ImportBeanDefinitionRegistrar} 的实现类，
 * Spring 容器在解析该类型的 Bean 时会调用其 {@link ImportBeanDefinitionRegistrar#registerBeanDefinitions(AnnotationMetadata, BeanDefinitionRegistry)} 方法，
 * 将 @{@link RpcComponentScan} 注解上的信息提取成 {@link AnnotationMetadata} 以及容器注册器对象作为此方法的参数，这个就是自定义注解式组件扫描的关键逻辑。
 * </p>
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
@Import(RpcBeanDefinitionRegistrar.class)
//重点内容：当解析到@RpcComponentScan 时，把 RpcBeanDefinitionRegistrar 这个类也导入进来，并调用它的注册逻辑
public @interface RpcComponentScan {

    /**
     * 扫描包路径
     */
    @AliasFor("basePackages")
    String[] value() default {};

    /**
     * 扫描包路径
     */
    @AliasFor("value")
    String[] basePackages() default {};

}
