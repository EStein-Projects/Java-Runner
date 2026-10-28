package edu.yu.cs.com3800.stage4;

import java.net.InetSocketAddress;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class LeaderSharer {

    private final ReentrantLock lock;
    private final Condition available;
    private volatile InetSocketAddress leader;

    public LeaderSharer() {
        lock = new ReentrantLock();
        available = lock.newCondition();
    }

    public void setLeader(InetSocketAddress newAddress) throws InterruptedException {
        lock.lockInterruptibly();
        try {
            leader = newAddress;
            available.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public InetSocketAddress getLeader() throws InterruptedException {
        InetSocketAddress curLeader = leader;
        while (curLeader == null) {
            waitForUpdate();
            curLeader = leader;
        }
        return curLeader;
    }

    private void waitForUpdate() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (leader == null) {
                available.await();
            }
        } finally {
            lock.unlock();
        }
    }
}