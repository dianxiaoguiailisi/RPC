package com.wxy.rpc.client.spring;

import com.wxy.rpc.client.annotation.RpcReference;
import com.wxy.rpc.client.proxy.ClientStubProxyFactory;
import com.wxy.rpc.core.exception.RpcException;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.InstantiationAwareBeanPostProcessor;
import org.springframework.beans.PropertyValues;

import java.lang.reflect.Field;

/**
 * 客户端 RPC 引用注入处理器。
 *
 * 这个类负责扫描消费方 Bean 中被 @RpcReference 标注的字段，并向字段注入远程服务代理对象。
 *
 * 原实现使用 BeanPostProcessor 的 postProcessAfterInitialization，在 Bean 初始化完成后再注入代理。
 * 这样虽然可以工作，但注入时机偏晚：如果业务 Bean 在 @PostConstruct 或 init 方法中使用远程服务字段，
 * 字段可能还没有完成注入。
 *
 * 当前实现改为 InstantiationAwareBeanPostProcessor 的 postProcessProperties，
 * 在 Spring 属性填充阶段完成 @RpcReference 注入，时机更接近 @Autowired，也更适合远程引用注入。
 *
 * 同时，这里不再直接临时创建代理对象，而是为每个远程引用创建 RpcReferenceBean。
 * RpcReferenceBean 是一个 FactoryBean，负责管理远程服务接口、版本号和代理对象创建逻辑。
 *
 * @author Wuxy
 * @version 1.0
 * @see com.wxy.rpc.client.annotation.RpcReference
 */
public class RpcClientBeanPostProcessor implements InstantiationAwareBeanPostProcessor, BeanFactoryAware {
    //代理工厂
    private final ClientStubProxyFactory proxyFactory;

    /**
     * Spring BeanFactory，用于注册和获取 RpcReferenceBean。
     */
    private ConfigurableListableBeanFactory beanFactory;

    public RpcClientBeanPostProcessor(ClientStubProxyFactory proxyFactory) {
        this.proxyFactory = proxyFactory;
    }

    /**
     * 注入 Spring BeanFactory。
     *
     * 这里需要 ConfigurableListableBeanFactory，是因为后续会把 RpcReferenceBean 注册成单例，
     * 再从 BeanFactory 中获取它生产出的远程代理对象。
     *
     * @param beanFactory Spring BeanFactory
     * @throws BeansException BeanFactory 类型不符合预期时抛出
     */
    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        if (!(beanFactory instanceof ConfigurableListableBeanFactory)) {
            throw new RpcException("RpcClientBeanPostProcessor requires ConfigurableListableBeanFactory.");
        }
        this.beanFactory = (ConfigurableListableBeanFactory) beanFactory;
    }

    /**
     * 在 Spring 属性填充阶段注入 @RpcReference 远程服务代理。
     *
     * 执行流程：
     * 1. 扫描当前 Bean 的所有字段。
     * 2. 找到被 @RpcReference 标注的字段。
     * 3. 解析远程服务接口类型和版本号。
     * 4. 创建或复用对应的 RpcReferenceBean。
     * 5. 从 RpcReferenceBean 获取远程代理对象，并反射注入字段。
     *
     * @param pvs 当前 Bean 的属性值
     * @param bean 当前正在进行属性填充的 Bean
     * @param beanName 当前 Bean 名称
     * @return 原属性值
     * @throws BeansException 注入异常
     */
    @Override
    public PropertyValues postProcessProperties(PropertyValues pvs, Object bean, String beanName) throws BeansException {
        // 获取该 bean 的类的所有属性（getFields - 获取所有的public属性，getDeclaredFields - 获取所有声明的属性，不区分访问修饰符）
        Field[] fields = bean.getClass().getDeclaredFields();
        // 遍历所有属性
        for (Field field : fields) {
            // 判断是否被 RpcReference 注解标注
            if (field.isAnnotationPresent(RpcReference.class)) {
                // 获得 RpcReference 注解
                RpcReference rpcReference = field.getAnnotation(RpcReference.class);
                // 默认类为属性当前类型
                // filed.class = java.lang.reflect.Field
                // filed.type = com.wxy.xxx.service.XxxService
                Class<?> clazz = field.getType();
                try {
                    // 如果指定了全限定类型接口名
                    if (!"".equals(rpcReference.interfaceName())) {
                        clazz = Class.forName(rpcReference.interfaceName());
                    }
                    // 如果指定了接口类型
                    if (rpcReference.interfaceClass() != void.class) {
                        clazz = rpcReference.interfaceClass();
                    }
                    // 创建或复用 RpcReferenceBean，并通过 FactoryBean 获取真正要注入的远程代理对象。
                    Object proxy = getReferenceProxy(clazz, rpcReference.version(), rpcReference.loadbalance());
                    // 关闭安全检查
                    field.setAccessible(true);
                    // 设置域的值为代理对象
                    field.set(bean, proxy);
                } catch (ClassNotFoundException | IllegalAccessException e) {
                    throw new RpcException(String.format("Failed to obtain proxy object, the type of field %s is %s, " +
                            "and the specified loaded proxy type is %s.", field.getName(), field.getClass(), clazz), e);
                }
            }
        }
        return pvs;
    }

    /**
     * 获取远程引用代理对象。
     *
     * 这里会先为接口和版本生成一个唯一的 ReferenceBean 名称。
     * 如果 BeanFactory 中还没有对应的 RpcReferenceBean，就注册一个单例。
     * 随后通过 beanFactory.getBean(referenceBeanName) 获取 FactoryBean 生产出的远程代理对象。
     *
     * @param clazz 远程服务接口类型
     * @param version 远程服务版本号
     * @param loadbalance 远程引用指定的负载均衡策略
     * @return 远程服务代理对象
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private Object getReferenceProxy(Class<?> clazz, String version, String loadbalance) {
        String effectiveLoadbalance = proxyFactory.resolveLoadBalanceName(loadbalance);
        String referenceBeanName = buildReferenceBeanName(clazz, version, effectiveLoadbalance);
        if (!beanFactory.containsSingleton(referenceBeanName)) {
            RpcReferenceBean referenceBean = new RpcReferenceBean(clazz, version, effectiveLoadbalance, proxyFactory);
            beanFactory.registerSingleton(referenceBeanName, referenceBean);
        }
        return beanFactory.getBean(referenceBeanName);
    }

    /**
     * 生成远程引用 Bean 名称。
     *
     * @param clazz 远程服务接口类型
     * @param version 远程服务版本号
     * @param loadbalance 负载均衡策略
     * @return ReferenceBean 名称
     */
    private String buildReferenceBeanName(Class<?> clazz, String version, String loadbalance) {
        return String.format("rpcReferenceBean:%s:%s:%s", clazz.getName(), version, loadbalance);
    }
}
