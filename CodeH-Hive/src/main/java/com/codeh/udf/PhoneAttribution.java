package com.codeh.udf;

import me.ihxq.projects.pna.PhoneNumberInfo;
import me.ihxq.projects.pna.PhoneNumberLookup;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.Optional;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className PhoneAttribution
 * @date 2021/3/29 17:44
 * @description 自定义获取手机归属地函数
 */
public class PhoneAttribution extends UDF {

    public static PhoneNumberLookup phoneNumberLookup = new PhoneNumberLookup();

    public static String evaluate(String phone) {

        Optional<String> invalidPhone = ValidPhone.getInvalidPhone(phone);

        if (!invalidPhone.isPresent()) {
            // 说明是无效手机号
            return null;
        }

        Optional<PhoneNumberInfo> lookup = phoneNumberLookup.lookup(phone.trim());
        if (lookup.isPresent()) {
            return lookup.get().getAttribution().getCity();
        }

        return null;
    }
}
