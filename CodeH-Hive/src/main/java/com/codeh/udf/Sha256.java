package com.codeh.udf;

import org.apache.commons.codec.digest.DigestUtils;

import java.io.UnsupportedEncodingException;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className Sha256
 * @date 2021/3/29 17:35
 * @description 自定义Sha256加密函数
 */
public class Sha256 {

    public static String evaluate(String str) {
        if (str != null && str != "") {
            try {
                byte[] bytes = str.getBytes("utf-8");
                /**
                 * sha256Hex：返回64位字符的十六进制字符串形式
                 */
                return DigestUtils.sha256Hex(bytes);
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }
        return null;
    }
}
