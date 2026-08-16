package com.buildathon.orchestrator.lock;

import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Distributed lock manager backed by Redisson. All cross-instance mutual
 * exclusion goes through here: scheduler leadership, singleton task execution,
 * per-run mutexes.
 */
@Component
public class LockManager {

    private static final Logger log = LoggerFactory.getLogger(LockManager.class);

    private final RedissonClient redisson;
    private final long leaseTimeMs;

    public LockManager(RedissonClient redisson, long leaseTimeMs) {
        this.redisson = redisson;
        this.leaseTimeMs = leaseTimeMs;
    }

    public RLock getLock(String name) {
        return redisson.getLock("orchestrator:lock:" + name);
    }

    /**
     * Runs the action while holding the lock. Returns null if the lock could
     * not be acquired within the wait time.
     */
    public <T> T withLock(String name, long waitMs, Supplier<T> action) {
        RLock lock = getLock(name);
        boolean acquired;
        try {
            acquired = lock.tryLock(waitMs, leaseTimeMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
        if (!acquired) {
            return null;
        }
        try {
            return action.get();
        } finally {
            try {
                lock.unlock();
            } catch (IllegalMonitorStateException e) {
                log.warn("Lock {} was already released (lease expired?)", name);
            }
        }
    }

    public void unlock(String name) {
        RLock lock = getLock(name);
        if (lock.isHeldByCurrentThread()) {
            lock.unlock();
        }
    }
}
