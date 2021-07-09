package com.codeh.linkedlist;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className DoubleLinkedListDemo
 * @date 2021/6/6 19:54
 * @description 双向链表的添加，删除，遍历........
 */
public class DoubleLinkedListDemo {
    public static void main(String[] args) {

        DoubleLinkedList doubleLinkedList = new DoubleLinkedList();

        DoubleNode node1 = new DoubleNode(1, "kafka");
        DoubleNode node2 = new DoubleNode(5, "spark");
        DoubleNode node3 = new DoubleNode(3, "scala");
        DoubleNode node4 = new DoubleNode(4, "flink");

        doubleLinkedList.addById(node1);
        doubleLinkedList.addById(node2);
        doubleLinkedList.addById(node3);

        // 更新前的遍历操作
        doubleLinkedList.list();

//        // 更新
//        doubleLinkedList.update(node4);
//        System.out.println("数据更新~~");

//        doubleLinkedList.del(node3);
//        System.out.println("删除后节点的数据~~");


//        doubleLinkedList.list();
//        doubleLinkedList.addById(node4);
//        doubleLinkedList.list();

    }
}

class DoubleLinkedList {

    // 头节点
    private DoubleNode head = new DoubleNode(0, "");

    /**
     * @return 返回头节点
     */
    public DoubleNode getHead() {
        return head;
    }

    /**
     * 遍历双向链表
     */
    public void list() {

        if (getHead().next == null) {
            System.out.println("双向链表为空~~");
            return;
        }

        DoubleNode tmp = head.next;

        while (tmp != null) {
            System.out.println(tmp);
            tmp = tmp.next;
        }
    }


    /**
     * 添加节点
     */
    public void add(DoubleNode node) {
        DoubleNode tmp = head;

        while (true) {

            if (tmp.next == null) {
                break;
            }
            tmp = tmp.next;
        }

        tmp.next = node;
        node.pre = tmp;
    }

    /**
     * @param node 更新节点
     */
    public void update(DoubleNode node) {
        if (head.next == null) {
            System.out.println("双向链表为空,更新的节点不存在~~");
            return;
        }

        DoubleNode tmp = head.next;

        while (true) {
            if (tmp == null) {
                System.out.println("需要更新的节点不存在~~");
                return;
            }

            // 查到要更新的节点，更新数据节点
            if (tmp.no == node.no) {
                break;
            }

            tmp = tmp.next;
        }

        // 更新节点的数据
        tmp.data = node.data;
    }

    /**
     * @param node 删除节点信息
     */
    public void del(DoubleNode node) {
        if (head.next == null) {
            System.out.println("链表为空");
            return;
        }

        DoubleNode tmp = head.next;
        Boolean flag = false;

        while (true) {
            if (tmp == null) {
                break;
            }

            if (tmp.no == node.no) {
                flag = true;
                break;
            }

            tmp = tmp.next;
        }

        if (flag) {
            tmp.pre.next = tmp.next;

            if (tmp.next != null) {
                tmp.next.pre = tmp.pre;
            }
        } else {
            System.out.println("删除的节点不存在~~");
        }

    }

    /**
     * @param node 通过id来添加元素
     */
    public void addById(DoubleNode node) {
        DoubleNode tmp = head;
        Boolean flag = false;

        while (true) {

            if (tmp.next == null) {
                break;
            }

            if (tmp.next.no > node.no) {
                break;
            } else if (tmp.next.no == node.no) {
                flag = true;
                break;
            }

            tmp = tmp.next;
        }

        if (flag) {
            System.out.println("当前节点已存在~~");
        } else {
            node.next = tmp.next; // 当前节点的下一个节点指向插入节点的下一个节点
            tmp.next = node;
            node.pre = tmp;
        }
    }

}


/**
 * 双向链表的实体
 */
class DoubleNode {
    public int no;
    public String data;
    public DoubleNode pre;
    public DoubleNode next;

    public DoubleNode(int no, String data) {
        this.no = no;
        this.data = data;
    }

    @Override
    public String toString() {
        return "DoubleNode{" +
                "no=" + no +
                ", data='" + data + '\'' +
                '}';
    }
}
