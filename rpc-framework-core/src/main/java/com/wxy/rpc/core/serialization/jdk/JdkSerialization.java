package com.wxy.rpc.core.serialization.jdk;

import com.wxy.rpc.core.exception.SerializeException;
import com.wxy.rpc.core.serialization.Serialization;

import java.io.*;

/**
 * JDK 序列化算法
 *
 * @author Wuxy
 * @version 1.0
 * @ClassName JdkSerialization
 * @Date 2023/1/5 12:24
 */
public class JdkSerialization implements Serialization {
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
            throw new SerializeException("Jdk serialize failed.", e);
        }
    }

    @Override
    public <T> void serialize(T object, OutputStream outputStream) {
        try {
            ObjectOutputStream oos = new ObjectOutputStream(outputStream);
            oos.writeObject(object);
            oos.flush();
        } catch (IOException e) {
            throw new SerializeException("Jdk serialize failed.", e);
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
            throw new SerializeException("Jdk deserialize failed.", e);
        }
    }

    @Override
    public <T> T deserialize(Class<T> clazz, InputStream inputStream) {
        try {
            ObjectInputStream ois = new ObjectInputStream(inputStream);
            return (T) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new SerializeException("Jdk deserialize failed.", e);
        }
    }
}
