package com.buildathon.orchestrator.scheduling;

import com.buildathon.orchestrator.lock.LockManager;
import org.redisson.api.RLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * Scheduler leadership via a Redisson lock. Only the instance holding
 * "scheduler-leader" scans cron schedules. The lease is renewed automatically
 * by Redisson's watchdog while held, and the lock is re-acquired on every
 * scan cycle.
 */
@Component
public class LeaderElection {

    public static final String LEADER_LOCK = "scheduler-leader";

    private static final Logger log = LoggerFactory.getLogger(LeaderElection.class);

    private final LockManager lockManager;

    public LeaderElection(LockManager lockManager) {
        this.lockManager = lockManager;
    }

    public boolean tryAcquire(long wait, TimeUnit unit) {
        RLock lock = lockManager.getLock(LEADER_LOCK);
        boolean acquired;
        try {
            acquired = lock.tryLock(wait, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
        if (acquired && !leadershipLogged) {
            log.info("Acquired scheduler leadership ({}:{})", hostname(), LEADER_LOCK);
            leadershipLogged = true;
        }
        return acquired;
    }

    private volatile boolean leadershipLogged;

    private String hostname() {
        try {
            return java.net.InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown";
        }
    }

    public void release() {
        lockManager.unlock(LEADER_LOCK);
    }

    public boolean isLeader() {
        RLock lock = lockManager.getLock(LEADER_LOCK);
        return lock.isHeldByCurrentThread();
    }
}
