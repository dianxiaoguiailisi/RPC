package com.wxy.rpc.core.serialization.json;

import com.google.gson.*;
import com.wxy.rpc.core.exception.SerializeException;
import com.wxy.rpc.core.serialization.Serialization;
import lombok.SneakyThrows;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

/**
 * 基于 Gson 库实现的 JSON 序列化算法类
 *
 * @author Wuxy
 * @version 1.0
 * @ClassName JsonSerialization
 * @Date 2023/1/5 12:23
 */
public class JsonSerialization implements Serialization {

    private static final Gson GSON = new GsonBuilder()
            .registerTypeAdapter(Class.class, new ClassCodec())
            .create();

    /**
     * 自定义 JavClass 对象序列化，解决 Gson 无法序列化 Class 信息
     */
    static class ClassCodec implements JsonSerializer<Class<?>>, JsonDeserializer<Class<?>> {
        @SneakyThrows
        @Override
        public Class<?> deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            String name = json.getAsString();
            return Class.forName(name);
        }

        @Override
        public JsonElement serialize(Class<?> src, Type typeOfSrc, JsonSerializationContext context) {
            // class -> json
            return new JsonPrimitive(src.getName());
        }
    }

    /** 
     * @param object
     * @return byte[]
     */
    @Override
    public <T> byte[] serialize(T object) {
        try {
            java.io.ByteArrayOutputStream outputStream = new java.io.ByteArrayOutputStream();
            serialize(object, outputStream);
            return outputStream.toByteArray();
        } catch (Exception e) {
            throw new SerializeException("Json serialize failed.", e);
        }
    }

    @Override
    public <T> void serialize(T object, OutputStream outputStream) {
        try {
            OutputStreamWriter writer = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8);
            GSON.toJson(object, writer);
            writer.flush();
        } catch (Exception e) {
            throw new SerializeException("Json serialize failed.", e);
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
            java.io.ByteArrayInputStream inputStream = new java.io.ByteArrayInputStream(bytes);
            return deserialize(clazz, inputStream);
        } catch (JsonSyntaxException e) {
            throw new SerializeException("Json deserialize failed.", e);
        }
    }

    @Override
    public <T> T deserialize(Class<T> clazz, InputStream inputStream) {
        try {
            InputStreamReader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
            return GSON.fromJson(reader, clazz);
        } catch (JsonSyntaxException e) {
            throw new SerializeException("Json deserialize failed.", e);
        }
    }
}
