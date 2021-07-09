package com.codeh.udf;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.UnsupportedEncodingException;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className Md5
 * @date 2021/3/29 17:08
 * @description 自定义md5加密函数
 */
public class Md5 extends UDF {

    // 必须要实现evaluate函数
    public static String evaluate(String str) {
        if (str != null && str != "") {
            try {
                byte[] bytes = str.getBytes("utf-8");
                /**
                 * DigestUtils: 属于commons-codec依赖里面的加密工具类
                 * md5Hex：计算MD5，并以32个字符的十六进制字符串形式返回值
                 */
                return DigestUtils.md5Hex(bytes);
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

}
