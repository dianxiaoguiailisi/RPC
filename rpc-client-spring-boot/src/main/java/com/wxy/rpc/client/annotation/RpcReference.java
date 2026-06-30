package com.wxy.rpc.client.annotation;

import java.lang.annotation.*;

/**
 * RPC 服务引用注解。标在 Controller 或 Service 的字段上。Spring 创建消费方 Bean 后，
 * RpcClientBeanPostProcessor 会扫描字段上的 @RpcReference，然后通过 ClientStubProxyFactory 创建远程服务代理对象，并注入到该字段。
 * 注意：这里注入的不是服务提供方的真实实现类，而是一个代理对象。
 * 调用该代理对象的方法时，会被动态代理拦截，最终通过服务发现、负载均衡和网络通信发起 RPC 调用。
 *
 * 示例：
 * @RpcReference
 * private HelloService helloService;
 */
@Target({ElementType.FIELD, ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface RpcReference {

    /**
     * 需要引用的远程服务接口类型。
     *
     * 如果不配置，框架通常会使用字段本身的类型作为接口类型。
     * 例如字段是 private HelloService helloService，就会默认引用 HelloService。
     */
    Class<?> interfaceClass() default void.class;

    /**
     * 需要引用的远程服务接口名，使用接口的全限定名。
     *
     * 例如 com.wxy.rpc.api.service.HelloService。
     * 如果 interfaceName 为空，则优先使用 interfaceClass 或字段类型。
     */
    String interfaceName() default "";

    /**
     * 服务版本号，默认 1.0。
     *
     * 客户端引用服务时，接口名和版本号会共同组成 serviceName。
     * 服务端注册服务时也使用同样的规则，所以两边版本号必须一致。
     */
    String version() default "1.0";

    /**
     * 负载均衡策略。
     *
     * 如果配置该字段，则当前远程引用优先使用注解上的负载均衡策略。
     * 如果不配置，则回退到 rpc.client.load-balance 全局配置。
     *
     * 可选值包括 random、roundRobin、consistentHash、adaptive。
     */
    String loadbalance() default "";

    /**
     * Mock 服务名称。
     *
     * 当前代码链路中没有完整实现 mock 调用逻辑，保留作服务降级或本地 mock 扩展点。
     */
    String mock() default "";

    /**
     * 服务调用超时时间。
     *
     * 当前项目主要通过 rpc.client.timeout 全局配置控制调用超时时间，
     * 该字段可作为后续按单个引用设置超时的扩展点。
     */
    int timeout() default 0;

}
