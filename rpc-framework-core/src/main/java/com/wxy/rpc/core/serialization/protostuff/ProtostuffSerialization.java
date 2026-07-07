package com.wxy.rpc.core.serialization.protostuff;

import com.wxy.rpc.core.exception.SerializeException;
import com.wxy.rpc.core.serialization.Serialization;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Protostuff 序列化算法实现类
 *
 * @author Wuxy
 * @version 1.0
 * @ClassName ProtostuffSerialization
 * @Date 2023/1/11 15:24
 */
public class ProtostuffSerialization implements Serialization {

    /**
     * Protostuff 的 LinkedBuffer 不是线程安全的。
     * 序列化器被缓存复用后，需要为每个线程单独保存 Buffer。
     */
    private static final ThreadLocal<LinkedBuffer> BUFFER_THREAD_LOCAL =
            ThreadLocal.withInitial(() -> LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE));

    private static final Map<Class<?>, Schema<?>> SCHEMA_CACHE = new ConcurrentHashMap<>();

    /** 
     * @param object
     * @return byte[]
     */
    @Override
    public <T> byte[] serialize(T object) {
        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            serialize(object, outputStream);
            return outputStream.toByteArray();
        } catch (Exception e) {
            throw new SerializeException("Protostuff serialize failed.", e);
        }
    }

    @Override
    public <T> void serialize(T object, OutputStream outputStream) {
        LinkedBuffer buffer = BUFFER_THREAD_LOCAL.get();
        try {
            Schema<T> schema = getSchema((Class<T>) object.getClass());
            ProtostuffIOUtil.writeTo(outputStream, object, schema, buffer);
        } catch (Exception e) {
            throw new SerializeException("Protostuff serialize failed.", e);
        } finally {
            // 重置 buffer
            buffer.clear();
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
            ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
            return deserialize(clazz, inputStream);
        } catch (Exception e) {
            throw new SerializeException("Protostuff deserialize failed.", e);
        }
    }

    @Override
    public <T> T deserialize(Class<T> clazz, InputStream inputStream) {
        try {
            Schema<T> schema = getSchema(clazz);
            T object = schema.newMessage();
            ProtostuffIOUtil.mergeFrom(inputStream, object, schema);
            return object;
        } catch (Exception e) {
            throw new SerializeException("Protostuff deserialize failed.", e);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> Schema<T> getSchema(Class<T> clazz) {
        return (Schema<T>) SCHEMA_CACHE.computeIfAbsent(clazz, RuntimeSchema::getSchema);
    }
}
