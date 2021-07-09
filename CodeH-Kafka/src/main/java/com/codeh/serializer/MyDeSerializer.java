package com.codeh.serializer;

import com.codeh.bean.Company;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className MyDeSerializer
 * @date 2021/4/1 17:28
 * @description 自定义反序列化器
 */
public class MyDeSerializer implements Deserializer<Company> {
    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public Company deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }

        String name = null, address = null;
        int nameLen, addressLen;

        try{
            ByteBuffer buffer = ByteBuffer.wrap(data);
            nameLen = buffer.getInt();
            byte[] nameBytes = new byte[nameLen];
            buffer.get(nameBytes);
            addressLen = buffer.getInt();
            byte[] addressBytes = new byte[addressLen];
            buffer.get(addressBytes);

            name = new String(nameBytes, "UTF-8");
            address = new String(addressBytes, "UTF-8");
        }catch (Exception e) {
            e.printStackTrace();
        }

        return new Company(name, address);
    }

    @Override
    public void close() {

    }
}
