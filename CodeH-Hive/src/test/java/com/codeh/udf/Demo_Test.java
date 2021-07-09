package com.codeh.udf;

import org.junit.Test;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className Md5_Test
 * @date 2021/3/29 17:25
 * @description 测试自定义Md5函数的功能
 */
public class Demo_Test {

    @org.junit.Test
    public void md5_test() {
        String md5_string = Md5.evaluate("15790349908");
        System.out.println(md5_string);
    }

    @Test
    public void sha256_test() {
        String sha256_string = Sha256.evaluate("1890000000");
        System.out.println(sha256_string);
    }

    @Test
    public void city() {
        String city = PhoneAttribution.evaluate("");
        System.out.println(city);
    }
}
