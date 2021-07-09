package com.codeh.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className CustomInterceptor
 * @date 2021/3/30 11:30
 * @description 自定义拦截器功能实现:根据Event值是否包含hello，对Event的Header进行设置不同的值
 */
public class MyInterceptor implements Interceptor {
    private List<Event> list;

    // 初始化操作
    public void initialize() {
        list = new ArrayList<Event>();
    }

    // 单个事件的处理
    public Event intercept(Event event) {
        // 1.获取事件头部信息
        Map<String, String> headers = event.getHeaders();

        // 2.获取事件的body信息
        String str = new String(event.getBody());

        // 3.判断事件是否包含hello
        if (str.contains("hello")) {
            headers.put("type", "hypers");
        } else {
            headers.put("type", "bigdata");
        }

        // 4.将事件返回
        return event;
    }

    // 多事件处理
    public List<Event> intercept(List<Event> events) {
        // 1.对全局的list集合进行清理
        list.clear();

        // 2.对多事件进行遍历
        for (Event event : events) {
            list.add(intercept(event));
        }
        // 3.返回结果
        return list;
    }

    public void close() {

    }

    // 定义一个静态内部类，保证拦截器的安全
    public static class Builder implements Interceptor.Builder {

        // 返回拦截器对象
        public Interceptor build() {
            return new MyInterceptor();
        }

        public void configure(Context context) {

        }
    }
}
