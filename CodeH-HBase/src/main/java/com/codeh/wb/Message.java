package com.codeh.wb;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className Message
 * @date 2021/5/14 10:17
 * @description 发布消息的实体类
 */
public class Message {
    private String uid;
    private String timestamp;
    private String content;

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    @Override
    public String toString() {
        return "Message{" +
                "uid='" + uid + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", content='" + content + '\'' +
                '}';
    }
}
