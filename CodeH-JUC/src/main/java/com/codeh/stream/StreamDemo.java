package com.codeh.stream;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className StreamDemo
 * @date 2021/11/5 11:43
 * @description 流式计算
 * 需求：一行代码实现下面的需求内容
 * 筛选下面的五个用户
 * 1.id必须式偶数
 * 2.年龄必须大于23岁
 * 3.用户名转为大写字母
 * 4.用户名字母倒着排序
 * 5.只能输出一个用户
 */
public class StreamDemo {
    public static void main(String[] args) {
        User user1 = new User(1, "a", 21);
        User user2 = new User(2, "b", 22);
        User user3 = new User(3, "c", 23);
        User user4 = new User(4, "d", 24);
        User user5 = new User(5, "e", 25);

        List<User> list = Arrays.asList(user1, user2, user3, user4, user5);
//        System.out.println(list);
        list.stream().filter(user -> user.getId() % 2 == 0)
                .filter(user -> user.getAge() > 23)
                .map(user -> user.getName().toUpperCase())
                .sorted(Comparator.reverseOrder())
                .limit(1)
                .forEach(System.out::println);
    }
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class User {
    private int id;
    private String name;
    private int age;
}