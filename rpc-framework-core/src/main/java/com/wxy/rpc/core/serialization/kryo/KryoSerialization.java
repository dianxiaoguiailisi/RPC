package com.wxy.rpc.core.serialization.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.wxy.rpc.core.common.RpcRequest;
import com.wxy.rpc.core.common.RpcResponse;
import com.wxy.rpc.core.exception.SerializeException;
import com.wxy.rpc.core.serialization.Serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Kryo 序列化算法
 * <p>
 * <a href="https://www.cnblogs.com/lxyit/p/12511645.html">相关简介</a><br>
 * <a href="https://github.com/EsotericSoftware/kryo">github地址</a>
 * </p>
 *
 * @author Wuxy
 * @version 1.0
 * @ClassName KryoSerialization
 * @Date 2023/1/6 15:22
 */
public class KryoSerialization implements Serialization {

    // kryo 线程不安全，所以使用 ThreadLocal 保存 kryo 对象
    private static final ThreadLocal<Kryo> KRYO_THREAD_LOCAL = ThreadLocal.withInitial(() -> {
        Kryo kryo = new Kryo();
        kryo.register(RpcRequest.class);
        kryo.register(RpcResponse.class);
        return kryo;
    });

    /** 
     * @param object
     * @return byte[]
     */
    @Override
    public <T> byte[] serialize(T object) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            serialize(object, baos);
            return baos.toByteArray();
        } catch (Exception e) {
            throw new SerializeException("Kryo serialize failed.", e);
        }
    }

    @Override
    public <T> void serialize(T object, OutputStream outputStream) {
        try {
            Output output = new Output(outputStream);
            Kryo kryo = KRYO_THREAD_LOCAL.get();
            // 将对象序列化为输出流
            kryo.writeObject(output, object);
            output.flush();
        } catch (Exception e) {
            throw new SerializeException("Kryo serialize failed.", e);
        } finally {
            KRYO_THREAD_LOCAL.get().reset();
        }
    }

    /** 
     * @param clazz
     * @param bytes
     * @return T
     */
    @Override
    public <T> T deserialize(Class<T> clazz, byte[] bytes) {
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            return deserialize(clazz, bais);
        } catch (Exception e) {
            throw new SerializeException("Kryo deserialize failed.", e);
        }
    }

    @Override
    public <T> T deserialize(Class<T> clazz, InputStream inputStream) {
        try {
            Input input = new Input(inputStream);
            Kryo kryo = KRYO_THREAD_LOCAL.get();
            // 将输入流反序列化为 T 对象
            return kryo.readObject(input, clazz);
        } catch (Exception e) {
            throw new SerializeException("Kryo deserialize failed.", e);
        } finally {
            KRYO_THREAD_LOCAL.get().reset();
        }
    }
}
