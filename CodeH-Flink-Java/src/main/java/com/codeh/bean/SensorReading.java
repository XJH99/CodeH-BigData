package com.codeh.bean;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className SensorReading
 * @date 2021/8/11 14:09
 * @description 流数据实体类
 */
public class SensorReading {
    private String id;
    private long timestamp;
    private double temperature;

    public SensorReading(String id, long timestamp, double temperature) {
        this.id = id;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "id='" + id + '\'' +
                ", timestamp=" + timestamp +
                ", temperature=" + temperature +
                '}';
    }
}
