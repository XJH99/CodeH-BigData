package com.codeh.udf;

import java.util.Optional;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className ValidPhone
 * @date 2021/3/29 17:52
 * @description 获取有效手机号
 */
public class ValidPhone {

    // Optional:是一个可以为null的容器对象;如果值存在则isPresent()方法会返回true，调用get()方法会返回该对象
    public static Optional<String> getInvalidPhone(String phone) {
        /**
         * 1.不能为空
         * 2.长度为11位
         * 3.正则匹配
         */
        if (phone != null && phone.trim().length() == 11 && phone.trim().matches("1[3|4|5|6|7|8|9][0-9]{9}")){
            return Optional.of(phone.trim());
        }

        return Optional.empty();
    }
}
