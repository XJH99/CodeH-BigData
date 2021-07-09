package com.codeh.linkedlist;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className Josep
 * @date 2021/6/6 21:26
 * @description 单项环形链表
 */
public class Josep {
    public static void main(String[] args) {
        CycleSingleLinkedList cycleSingleLinkedList = new CycleSingleLinkedList();
        cycleSingleLinkedList.add(100);
        cycleSingleLinkedList.show();
        cycleSingleLinkedList.countBoy(10,20,100);

    }
}

/**
 * 环形单项链表
 */
class CycleSingleLinkedList {

    private Boy first = null;

    /**
     * @param nums 添加节点数据
     */
    public void add(int nums) {
        if (nums < 1) {
            System.out.println("nums 的值不正确");
            return;
        }

        Boy cur = null; // 辅助指针，用于构建环形链表
        for (int i = 1; i <= nums; i++) {
            Boy boy = new Boy(i);

            if (i == 1) {
                first = boy;
                first.setNext(first);   // 构成环
                cur = first;
            } else {
                cur.setNext(boy);
                boy.setNext(first);
                cur = boy;
            }
        }
    }

    /**
     * 遍历数据节点
     */
    public void show() {
        if (first == null) {
            System.out.println("没有任何节点~~");
            return;
        }

        Boy curBoy = first;
        while (true) {
            System.out.printf("小孩的编号为 %d\n", curBoy.no);
            if (curBoy.getNext() == first) {
                break;
            }
            curBoy = curBoy.getNext(); // curBoy节点后移操作
        }
    }

    /**
     * 根据用户的输入，计算小孩出圈的顺序
     * @param startNo 表示从第几个小孩开始数数
     * @param countNum 表示数几下
     * @param nums 表示最初有多少个小孩在圈中
     */
    public void countBoy(int startNo, int countNum, int nums) {
        if (first ==  null || startNo < 1 || startNo > nums) {
            System.out.println("参数输入有误，请重新输入~~");
            return;
        }

        Boy curBoy = first; // 辅助节点

        while (true) {
            if (curBoy.getNext() == first) { // 使它指向最后一个节点
                break;
            }

            curBoy = curBoy.getNext();
        }

        // 报数前，先让first和curBoy移动k-1次
        for (int j=0; j<startNo-1; j++) {
            first = first.getNext();
            curBoy = curBoy.getNext();
        }

        while (true) {

            if (curBoy == first) { // 圈中只有一个节点
                break;
            }

            // 让first和curBoy指针同时移动countNum-1
            for (int j=0; j<countNum-1; j++) {
                first = first.getNext();
                curBoy = curBoy.getNext();
            }

            // 这时first指向的节点，就是出圈小孩的节点
            System.out.printf("小孩%d出圈\n",first.getNo());
            first = first.getNext();
            curBoy.setNext(first);
        }
        System.out.printf("最后留在圈中的小孩编号%d\n", first.getNo());

    }

}

class Boy {
    public int no;
    public Boy next;

    public Boy(int no) {
        this.no = no;
    }

    public int getNo() {
        return no;
    }

    public void setNo(int no) {
        this.no = no;
    }

    public Boy getNext() {
        return next;
    }

    public void setNext(Boy next) {
        this.next = next;
    }
}
