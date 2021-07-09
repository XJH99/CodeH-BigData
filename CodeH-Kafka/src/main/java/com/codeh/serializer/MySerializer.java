package com.codeh.serializer;

import com.codeh.bean.Company;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className MySerializer
 * @date 2021/3/30 16:25
 * @description 自定义序列化器
 */
public class MySerializer implements Serializer<Company> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Company data) {
        if (data == null) {
            return null;
        }

        byte[] name, address;


        try {
            if (data.getName() != null) {
                name = data.getName().getBytes("UTF-8");
            } else {
                name = new byte[0];
            }

            if (data.getAddress() != null) {
                address = data.getAddress().getBytes("UTF-8");
            } else {
                address = new byte[0];
            }

            ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + name.length + address.length);
            buffer.put(name);
            buffer.putInt(name.length);
            buffer.put(address);
            buffer.putInt(address.length);
            return buffer.array();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        return new byte[0];
    }

    @Override
    public void close() {

    }
}
