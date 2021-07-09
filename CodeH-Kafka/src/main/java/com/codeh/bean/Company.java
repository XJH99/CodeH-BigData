package com.codeh.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className Company
 * @date 2021/3/30 16:21
 * @description 构建一个简单的实体类
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Company {
    private String name;
    private String address;
}
