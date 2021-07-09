package com.codeh.hashtab;

import java.util.Scanner;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className HashTableDemo
 * @date 2021/7/7 17:50
 * @description 自定义实现哈希表的功能
 * 哈希表是由数组与链表组成的，数组中的每一个元素代表着一条链表
 */
public class HashTableDemo {
    public static void main(String[] args) {
        // 创建hash表
        HashTab hashTab = new HashTab(7);

        String key = "";
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.println("add: 添加雇员");
            System.out.println("list: 显示雇员");
            System.out.println("find: 查找雇员");
            System.out.println("exit: 退出系统");
            key = scanner.next();
            switch (key) {
                case "add":
                    System.out.println("请输入雇员编号");
                    int id = scanner.nextInt();
                    System.out.println("请输入雇员姓名");
                    String name = scanner.next();
                    // 创建雇员
                    Emp emp = new Emp(id, name);
                    hashTab.add(emp);
                    break;
                case "list":
                    hashTab.list();
                    break;
                case "find":
                    System.out.println("请输入要查找的雇员id");
                    id = scanner.nextInt();
                    hashTab.findById(id);
                    break;
                case "exit":
                    scanner.close();
                    System.exit(0);
                default:
                    break;
            }
        }

    }
}

/**
 * hash表的实际业务处理类，HashTab管理多条链表
 */
class HashTab {
    private EmpLinkedList[] empLinkedLists;
    private int size;

    public HashTab(int size) {
        this.size = size;
        empLinkedLists = new EmpLinkedList[size];
        // 注意: 这里要对数组中每一个链表初始化,不然会出现空指针的情况
        for (int i = 0; i < size; i++) {
            empLinkedLists[i] = new EmpLinkedList();
        }
    }

    /**
     * 添加雇员
     *
     * @param emp 添加的雇员
     */
    public void add(Emp emp) {
        // 获取链表编号
        int linkedNo = hashFun(emp.id);

        // 将雇员添加到对应的链表中
        empLinkedLists[linkedNo].add(emp);
    }

    /**
     * 遍历链表
     */
    public void list() {
        for (int i = 0; i < size; i++) {
            empLinkedLists[i].list(i);
        }
    }

    /**
     * @param id 雇员id
     */
    public void findById(int id) {
        int linkedNo = hashFun(id);

        Emp emp = empLinkedLists[linkedNo].findById(id);

        if (emp != null) {
            // 说明找到了
            System.out.printf("在第%d条链表中找到了编号为id=%d的雇员\t", (linkedNo + 1), id);
            System.out.println();
        } else {
            System.out.println("在hash表中没有找到该雇员");
        }
    }


    /**
     * hash算法
     *
     * @param id 雇员id
     * @return 对应的链表编号
     */
    public int hashFun(int id) {
        return id % size;
    }
}

/**
 * 雇员实体
 */
class Emp {
    public int id;
    public String name;
    public Emp next;

    public Emp(int id, String name) {
        this.id = id;
        this.name = name;
    }
}

class EmpLinkedList {
    // 头指针，没有头节点，直接指向第一个节点
    private Emp head;

    /**
     * 添加
     *
     * @param emp 加入的雇员
     */
    public void add(Emp emp) {
        // 头节点为空，表示没有元素
        if (head == null) {
            head = emp;
            return;
        }

        // 使用一个辅助节点完成赋值操作
        Emp curEmp = head;
        while (true) {
            if (curEmp.next == null) {
                break;
            }
            curEmp = curEmp.next;
        }

        curEmp.next = emp;
    }

    /**
     * 遍历链表
     *
     * @param no 编号
     */
    public void list(int no) {
        if (head == null) {
            System.out.println("第" + (no + 1) + "条链表为空");
            return;
        }

        System.out.print("第" + (no + 1) + "条链表的信息为");
        Emp curEmp = head;
        while (true) {
            System.out.printf(" => id=%d name=%s\t", curEmp.id, curEmp.name);
            if (curEmp.next == null) {
                break;
            }
            curEmp = curEmp.next; // 节点后移
        }
        System.out.println();

    }

    /**
     * 查找雇员
     *
     * @param id 雇员id
     * @return 雇员信息
     */
    public Emp findById(int id) {
        if (head == null) {
            System.out.println("链表为空");
            return null;
        }

        Emp curEmp = head;
        while (true) {
            if (curEmp.id == id) {
                break;
            }
            if (curEmp.next == null) { // 遍历当前节点没有找到雇员
                curEmp = null;
                break;
            }

            curEmp = curEmp.next; // 节点后移
        }

        return curEmp;
    }
}
