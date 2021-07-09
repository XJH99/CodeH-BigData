package com.codeh.linkedlist;

import java.util.Queue;
import java.util.Stack;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className SingleLinkedListDemo
 * @date 2021/6/2 18:56
 * @description 单链表操作
 */
public class SingleLinkedListDemo {

    public static void main(String[] args) {
        SingleLinkedList singleLinkedList = new SingleLinkedList();

        Node node1 = new Node(1, "张三");
        Node node2 = new Node(3, "李四");
        Node node3 = new Node(2, "王五");
        Node node4 = new Node(2, "李白");

        // 添加节点
        singleLinkedList.addNode(node1);
        singleLinkedList.addNode(node2);
        singleLinkedList.addNode(node3);

//        singleLinkedList.addNodeById(node1);
//        singleLinkedList.addNodeById(node2);
//        singleLinkedList.addNodeById(node3);

//        final long qty = SingleLinkedListDemo.getValidNodeQty(singleLinkedList.head);
//        System.out.println(qty);

//        SingleLinkedListDemo.reversePrint(singleLinkedList.head);
//        System.out.println("修改前的节点信息~~~");
//        singleLinkedList.list();
//
//
//        singleLinkedList.deleteNode(2);
//        System.out.println("删除后的节点信息~~~");
//        singleLinkedList.list();

    }

    /**
     * 获取有效节点的个数
     *
     * @return
     */
    public static long getValidNodeQty(Node head) {
        if (head.next == null) {
            return 0; // 空链表
        }
        long count = 0;

        Node tmp = head.next;

        while (tmp != null) {
            count++;

            tmp = tmp.next;
        }
        return count;
    }

    /**
     * 查找单链表中的倒数第k个节点
     * @param head
     * @param index
     * @return
     */
    public static Node findLastIndexNode(Node head, int index) {
        if(head.next == null) {
            return null; // 空链表，没有找到
        }

        // 获取有效节点的个数
        long length = getValidNodeQty(head);

        // 做一个索引的校验判断
        if (index <=0 || index > length) {
            return null;
        }

        Node tmp = head.next;

        for (int i=0; i<length-index; i++) {
            tmp = tmp.next;
        }

        return tmp;
    }


    /**
     * 链表的反转
     * @param head
     */
    public static void reverseList(Node head) {

        if (head.next == null || head.next.next == null) {
            System.out.println("该链表为空或只有一个节点~~");
             return;
        }

        Node cur = head.next;   // 临时节点
        Node next = null;   // 临时节点的下一个节点
        // 定义一个新的节点
        Node newHead = new Node(0, "");

        while (cur != null) {
            next = cur.next;    // 将临时节点的下一个节点保存
            cur.next = newHead.next; // 加入的临时节点指向新链表头节点的下一个节点
            newHead.next = cur; // 新链表的头节点指向新加入的临时节点
            cur = next; // 将保存的原始链表的下一个节点赋值给当前节点
        }
        head.next = newHead.next; // 原始头节点的下一个节点指向新链表头节点的下一个节点
    }


    /**
     * 逆向遍历链表
     * @param head
     */
    public static void reversePrint(Node head) {
        if (head.next == null) {
            return;
        }

        Node tmp = head.next;
        // 栈：先进后出
        Stack<Node> stack = new Stack<>();

        while (tmp != null) {
            stack.push(tmp);

            tmp = tmp.next;
        }

        // 当栈中还有元素存在时，出栈操作
        while (stack.size() > 0) {
            System.out.println(stack.pop()); // 出栈操作
        }

    }


}

class SingleLinkedList {

    // 初始化一个头节点，不存放任何具体的数据
    public static Node head = new Node(0, "");



    /**
     * 不考虑编号时，向单向链表中添加节点数据
     *
     * @param node
     */
    public void addNode(Node node) {
        // 头节点不能动，赋值给一个临时节点
        Node tmp = head;

        while (true) {
            // 尾节点时退出
            if (tmp.next == null) {
                break;
            }
            tmp = tmp.next;
        }

        // 退出循环后，将加入的节点挂载在链表最后
        tmp.next = node;
    }

    /**
     * 考虑编号，将该节点插入到链表的相应位置
     *
     * @param node
     */
    public void addNodeById(Node node) {
        Node tmp = head;

        // 用于判断当前该节点是否存在
        Boolean flag = false;

        while (true) {
            // 已经到链表最后了
            if (tmp.next == null) {
                break;
            }

            // 符合该条件就插入到tmp节点后面
            if (tmp.next.id > node.id) {
                break;
            } else if (tmp.next.id == node.id) {
                flag = true; // 说明编号已存在
                break;
            }

            tmp = tmp.next;
        }

        if (flag) {
            System.out.printf("准备插入的节点 %d 已存在，不能加入\n", tmp.id);
        } else {
            // 插入到tmp后面
            node.next = tmp.next;   // 插入节点的指针指向tmp的下一个节点
            tmp.next = node;    // tmp的指针指向node节点
        }
    }

    /**
     * 修改节点信息，只修改内容不修改编号
     * @param node
     */
    public void updateNode(Node node) {
        Node tmp = head;

        if (tmp.next == null) {
            System.out.println("链表为空");
            return;
        }

        // 用于判断链表是否存在修改的节点
        Boolean flag = false;

        while (true) {
            if (tmp.next == null) {
                break;
            }

            // 存在该节点，并修改节点信息返回状态
            if (tmp.next.id == node.id) {
                tmp.next.name = node.name;
                flag = true;
                break;
            }

            tmp = tmp.next;
        }

        if (flag) {
            System.out.printf("节点 %d 信息已修改", node.id);
        } else {
            System.out.printf("修改的节点 %d 号在链表中不存在", node.id);
        }
    }

    /**
     * 删除节点信息
     * @param id
     */
    public void deleteNode(int id) {
        if (head.next == null) {
            System.out.println("链表为空~");
            return;
        }

        Node tmp = head;
        Boolean flag = false;

        while (true) {
            if (tmp.next == null) {
                break;
            }

            if (tmp.next.id == id) {
                flag = true;
                break;
            }

            tmp = tmp.next;
        }

        if (flag) {
            tmp.next = tmp.next.next;
        } else {
            System.out.printf("你要删除的 %d 节点在链表中不存在", id);
        }
    }

    /**
     * 遍历链表
     */
    public void list() {

        if (head.next == null) {
            System.out.println("链表为空~~~");
            return;
        }

        Node tmp = head.next;

        while (true) {
            if (tmp == null) {
                break;
            }
            // 输出节点信息
            System.out.println(tmp);
            tmp = tmp.next;
        }
    }


}


/**
 * 声明一个节点实例
 */
class Node {
    public int id;
    public String name;
    public Node next;  // 指向下一个节点

    public Node(int id, String name) {
        this.id = id;
        this.name = name;
    }

    @Override
    public String toString() {
        return "Node{" +
                "id=" + id +
                ", name='" + name +
                '}';
    }
}




