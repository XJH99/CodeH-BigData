package com.codeh.sync_lock;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className SaleTicketDemo02
 * @date 2021/10/29 18:05
 * @description TODO
 */
public class SaleTicketDemo02 {
    public static void main(String[] args) {
        Ticket2 ticket = new Ticket2();
        new Thread(() -> {for (int i = 0; i < 30; i++) ticket.sale();}, "A").start();
        new Thread(() -> {for (int i = 0; i < 30; i++) ticket.sale();}, "B").start();
        new Thread(() -> {for (int i = 0; i < 30; i++) ticket.sale();}, "C").start();
    }
}

/**
 * lock三部曲
 * 1：创建锁对象
 * 2. 加锁
 * 3. 释放锁
 */
class Ticket2 {
    private int ticket_num = 20;

    // 创建lock锁对象
    Lock lock = new ReentrantLock();

    public void sale() {
        // 加锁
        lock.lock();
        
        try {
            if (ticket_num > 0) {
                System.out.println("线程" + Thread.currentThread().getName() + "售出第" + (ticket_num--) + "张票，余票数为：" + ticket_num);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 释放锁
            lock.unlock();
        }
    }
}