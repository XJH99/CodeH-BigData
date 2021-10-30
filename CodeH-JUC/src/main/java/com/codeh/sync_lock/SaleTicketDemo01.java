package com.codeh.sync_lock;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className SaleTicketDemo01
 * @date 2021/10/29 17:39
 * @description synchronized案例使用
 */
public class SaleTicketDemo01 {
    public static void main(String[] args) {

        // 并发：多个线程操作同一个资源类，把资源丢入到线程
        Ticket1 ticket = new Ticket1();
        new Thread(() -> {
            for (int i = 0; i < 30; i++) {
                ticket.sale();
            }
        }, "A").start();
        new Thread(() -> {
            for (int i = 0; i < 30; i++) {
                ticket.sale();
            }
        }, "B").start();
        new Thread(() -> {
            for (int i = 0; i < 30; i++) {
                ticket.sale();
            }
        }, "B").start();

    }
}

class Ticket1 {
   private int ticket_num = 20;

   // synchronized的本质：队列，锁
    public synchronized void sale() {
        if (ticket_num > 0) {
            System.out.println("线程" + Thread.currentThread().getName() + "售出第" + (ticket_num--) + "张票，余票数为：" + ticket_num);
        }
    }

}
