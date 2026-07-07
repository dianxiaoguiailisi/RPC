package com.wxy.rpc.core.serialization;

import com.wxy.rpc.core.exception.SerializeException;
import com.wxy.rpc.core.extension.SPI;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

@SPI
public interface Serialization {

    /**
     * 将传入对象进行序列化
     *
     * @param object 需要被序列化的对象
     * @param <T>    对象类型
     * @return 返回序列化后的字节数组
     */
    <T> byte[] serialize(T object);

    /**
     * 将传入对象序列化后直接写入输出流。
     *
     * 默认实现会退化为 byte[]，具体序列化器可以覆盖成真正的流式写入。
     *
     * @param object       需要被序列化的对象
     * @param outputStream 输出流
     * @param <T>          对象类型
     */
    default <T> void serialize(T object, OutputStream outputStream) {
        try {
            outputStream.write(serialize(object));
        } catch (IOException e) {
            throw new SerializeException("Serialize to output stream failed.", e);
        }
    }

    /**
     * 将对象进行反序列化
     *
     * @param clazz 对象的类型
     * @param bytes 对象字节数组
     * @param <T>   对象类型
     * @return 返回序列化后的对象
     */
    <T> T deserialize(Class<T> clazz, byte[] bytes);

    /**
     * 直接从输入流中反序列化对象。
     *
     * 默认实现会先读取成 byte[]，具体序列化器可以覆盖成真正的流式读取。
     *
     * @param clazz       对象的类型
     * @param inputStream 输入流
     * @param <T>         对象类型
     * @return 返回反序列化后的对象
     */
    default <T> T deserialize(Class<T> clazz, InputStream inputStream) {
        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int length;
            while ((length = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, length);
            }
            return deserialize(clazz, outputStream.toByteArray());
        } catch (IOException e) {
            throw new SerializeException("Deserialize from input stream failed.", e);
        }
    }

}
