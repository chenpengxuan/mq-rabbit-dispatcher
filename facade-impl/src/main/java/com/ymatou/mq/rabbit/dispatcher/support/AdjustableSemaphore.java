/**
 * (C) Copyright 2016 Ymatou (http://www.ymatou.com/).
 *
 * All rights reserved.
 */
package com.ymatou.mq.rabbit.dispatcher.support;

import java.io.Serializable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * 可以动态调整许可证的Semaphore
 * 
 * @author tony 2016年8月16日 下午11:17:50
 *
 */
final public class AdjustableSemaphore implements Serializable {

    private static final long serialVersionUID = -3816594668972551560L;

    private final ResizeableSemaphore semaphore;
    private int maxPermits = 0;

    public AdjustableSemaphore(int permits) {
        maxPermits = permits;
        semaphore = new ResizeableSemaphore(permits);
    }

    public synchronized void setMaxPermits(int newMax) {
        if (newMax < 1) {
            throw new IllegalArgumentException("Semaphore size must be at least 1,"
                    + " was " + newMax);
        }
        int delta = newMax - this.maxPermits;
        if (delta == 0) {
            return;
        } else if (delta > 0) {
            this.semaphore.release(delta);
        } else {
            delta *= -1;
            this.semaphore.reducePermits(delta);
        }
        this.maxPermits = newMax;
    }

    public void release() {
        this.semaphore.release();
    }

    public void acquire() throws InterruptedException {
        this.semaphore.acquire();
    }

    public void tryAcquire(long timeout) throws InterruptedException {
        boolean acquired = this.semaphore.tryAcquire(timeout, TimeUnit.MILLISECONDS);
        if(!acquired){
            throw new InterruptedException("acquire somaphore timeout interrupted");
        }
    }

    public int availablePermits() {
        return this.semaphore.availablePermits();
    }

    private static final class ResizeableSemaphore extends Semaphore {
        private static final long serialVersionUID = 1L;

        ResizeableSemaphore(int permits) {
            super(permits);
        }

        @Override
        protected void reducePermits(int reduction) {
            super.reducePermits(reduction);
        }
    }

    /**
     * @return the maxPermits
     */
    public synchronized int getMaxPermits() {
        return maxPermits;
    }
}
