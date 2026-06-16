// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.catalog;

import org.apache.doris.common.DdlException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Verifies the snapshot quiesce lock semantics used in Env.acquireDdlPermit(),
 * Env.releaseDdlPermit(), and Env.quiesceForSnapshot().
 *
 * Env cannot be directly instantiated in a unit test (its constructor starts
 * threads and requires FE infrastructure). Instead these tests verify the exact
 * Java lock primitives chosen for the implementation:
 *
 *   ReentrantReadWriteLock(fair=true)
 *   readLock().tryLock(0, TimeUnit.SECONDS)   — used in acquireDdlPermit()
 *   writeLock().tryLock(timeout, SECONDS)     — used in quiesceForSnapshot()
 *
 * The key invariant under test:
 *   tryLock(0, SECONDS) on the READ side respects a waiting WRITE lock.
 *   This prevents starvation of the snapshot operation.
 *   The non-timed tryLock() would barge past a waiting writer — WRONG.
 */
public class EnvSnapshotQuiesceTest {

    // ── Helpers that replicate Env's three methods ────────────────────────────

    /** Replicates Env.snapshotQuiesceLock field. */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    /** Replicates Env.acquireDdlPermit() — fails fast if WRITE lock is waiting or held. */
    void acquireDdlPermit() throws DdlException {
        try {
            if (!lock.readLock().tryLock(0, TimeUnit.SECONDS)) {
                throw new DdlException(
                        "DDL is temporarily paused while a cluster snapshot is being taken.");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DdlException("DDL permit acquisition interrupted");
        }
    }

    /** Replicates Env.releaseDdlPermit(). */
    void releaseDdlPermit() {
        lock.readLock().unlock();
    }

    /** Replicates Env.quiesceForSnapshot(). */
    void quiesceForSnapshot(Env.QuiesceRunnable criticalSection) throws Exception {
        boolean acquired;
        try {
            acquired = lock.writeLock().tryLock(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DdlException("Snapshot quiesce interrupted");
        }
        if (!acquired) {
            throw new DdlException("Snapshot quiesce timed out");
        }
        try {
            criticalSection.run();
        } finally {
            lock.writeLock().unlock();
        }
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    /**
     * Baseline: DDL can acquire the READ permit when no snapshot is in progress.
     */
    @Test
    public void testAcquirePermitSucceedsNormally() throws Exception {
        acquireDdlPermit();
        releaseDdlPermit();
    }

    /**
     * Core correctness: DDL is denied when the snapshot holds the WRITE lock.
     * Verifies that tryLock(0, SECONDS) correctly returns false when WRITE is held.
     */
    @Test
    public void testDdlDeniedWhileSnapshotWriteLockHeld() throws Exception {
        CountDownLatch writeLockHeld = new CountDownLatch(1);
        CountDownLatch ddlTried = new CountDownLatch(1);
        AtomicBoolean ddlWasDenied = new AtomicBoolean(false);

        // Snapshot thread: acquires WRITE lock and holds it
        Thread snapshotThread = new Thread(() -> {
            try {
                quiesceForSnapshot(() -> {
                    writeLockHeld.countDown();           // signal: WRITE lock is now held
                    ddlTried.await(5, TimeUnit.SECONDS); // wait for DDL attempt
                });
            } catch (Exception e) {
                // unexpected
            }
        });

        // DDL thread: tries READ lock while WRITE is held
        Thread ddlThread = new Thread(() -> {
            try {
                writeLockHeld.await(5, TimeUnit.SECONDS);
                acquireDdlPermit(); // must throw — WRITE is held
            } catch (DdlException e) {
                ddlWasDenied.set(true); // expected
            } catch (Exception e) {
                // unexpected
            } finally {
                ddlTried.countDown();
            }
        });

        snapshotThread.start();
        ddlThread.start();
        snapshotThread.join(10_000);
        ddlThread.join(5_000);

        Assertions.assertTrue(ddlWasDenied.get(),
                "DDL permit must be denied while snapshot WRITE lock is held");
    }

    /**
     * Core correctness: tryLock(0, SECONDS) also fails when WRITE lock is only
     * WAITING (not yet held). This is the starvation-prevention property of the
     * fair lock — without it the snapshot could never acquire the WRITE lock.
     *
     * Note: non-timed tryLock() would barge past a waiting writer (Java spec).
     * tryLock(0, SECONDS) correctly respects the fair queue.
     */
    @Test
    public void testDdlDeniedWhileSnapshotIsWaiting() throws Exception {
        CountDownLatch ddlHoldsRead = new CountDownLatch(1);
        // Snapshot signals when it has started waiting for the WRITE lock
        CountDownLatch snapshotQueuedForWrite = new CountDownLatch(1);
        AtomicBoolean newDdlDenied = new AtomicBoolean(false);
        AtomicBoolean snapshotSucceeded = new AtomicBoolean(false);

        // DDL Thread 1: holds READ lock while snapshot waits
        Thread ddl1 = new Thread(() -> {
            try {
                acquireDdlPermit();
                ddlHoldsRead.countDown();
                // Wait until snapshot is confirmed queued for write, then release
                snapshotQueuedForWrite.await(5, TimeUnit.SECONDS);
                Thread.sleep(20); // brief hold after snapshot is queued
                releaseDdlPermit();
            } catch (Exception e) {
                // unexpected
            }
        });

        // Snapshot thread: waits for WRITE lock and signals when queued
        Thread snapshotThread = new Thread(() -> {
            try {
                ddlHoldsRead.await(5, TimeUnit.SECONDS);
                // Use a background thread to signal once snapshot is waiting
                // We signal via hasQueuedThreads() polling
                Thread signaller = new Thread(() -> {
                    // Spin until the snapshot thread is queued for the write lock
                    for (int i = 0; i < 100 && !lock.hasQueuedThreads(); i++) {
                        try {
                            Thread.sleep(5);
                        } catch (InterruptedException e) {
                            break;
                        }
                    }
                    snapshotQueuedForWrite.countDown();
                });
                signaller.setDaemon(true);
                signaller.start();
                quiesceForSnapshot(() -> snapshotSucceeded.set(true));
            } catch (Exception e) {
                // unexpected
            }
        });

        // DDL Thread 2: tries READ after snapshot is confirmed queued — must fail
        Thread ddl2 = new Thread(() -> {
            try {
                snapshotQueuedForWrite.await(5, TimeUnit.SECONDS);
                acquireDdlPermit();
            } catch (DdlException e) {
                newDdlDenied.set(true); // expected
            } catch (Exception e) {
                // unexpected
            }
        });

        ddl1.start();
        snapshotThread.start();
        ddl2.start();
        ddl1.join(5_000);
        snapshotThread.join(5_000);
        ddl2.join(5_000);

        Assertions.assertTrue(newDdlDenied.get(),
                "New DDL must be denied while snapshot WRITE lock is waiting (fair queue)");
        Assertions.assertTrue(snapshotSucceeded.get(),
                "Snapshot must succeed after DDL 1 releases READ lock");
    }

    /**
     * quiesceForSnapshot waits for all in-flight DDL to finish before running the
     * critical section. The WRITE lock only acquires when all READ locks are released.
     */
    @Test
    public void testQuiesceWaitsForInFlightDdl() throws Exception {
        CountDownLatch ddlHoldsLock = new CountDownLatch(1);
        CountDownLatch snapshotStarted = new CountDownLatch(1);
        AtomicBoolean ddlCompletedBeforeCriticalSection = new AtomicBoolean(false);
        AtomicBoolean criticalSectionRan = new AtomicBoolean(false);

        Thread ddlThread = new Thread(() -> {
            try {
                acquireDdlPermit();
                ddlHoldsLock.countDown();
                snapshotStarted.await(5, TimeUnit.SECONDS);
                Thread.sleep(50); // simulate DDL work
                // Set BEFORE releasing the lock — if critical section ran before this,
                // the assertion below would catch it
                ddlCompletedBeforeCriticalSection.set(true);
                releaseDdlPermit();
            } catch (Exception e) {
                // unexpected
            }
        });

        Thread snapshotThread = new Thread(() -> {
            try {
                ddlHoldsLock.await(5, TimeUnit.SECONDS);
                snapshotStarted.countDown();
                // Critical section runs ONLY after DDL releases READ lock.
                // We do NOT assert inside the lambda — AssertionError would be
                // caught by the catch(Exception) block and silently swallowed.
                quiesceForSnapshot(() -> criticalSectionRan.set(true));
            } catch (Exception e) {
                // unexpected
            }
        });

        ddlThread.start();
        snapshotThread.start();
        ddlThread.join(5_000);
        snapshotThread.join(5_000);

        // Assert OUTSIDE the lambda — these run on the test thread and are not swallowed
        Assertions.assertTrue(criticalSectionRan.get(),
                "snapshot critical section must run after DDL completes");
        Assertions.assertTrue(ddlCompletedBeforeCriticalSection.get(),
                "DDL must complete (flag set) before critical section ran");
    }

    /**
     * Exception thrown inside the critical section propagates out of quiesceForSnapshot.
     * The WRITE lock must still be released even when the critical section throws.
     */
    @Test
    public void testQuiesceExceptionReleasesLockAndPropagates() {
        Assertions.assertThrows(DdlException.class, () ->
                quiesceForSnapshot(() -> {
                    throw new DdlException("intentional test error");
                })
        );

        // WRITE lock must have been released — DDL can now proceed
        Assertions.assertDoesNotThrow(() -> {
            acquireDdlPermit();
            releaseDdlPermit();
        }, "READ lock must be acquirable after snapshot critical section throws");
    }

    /**
     * Concurrent snapshot attempts: if one snapshot holds the WRITE lock, a second
     * snapshot attempt with a short timeout must fail.
     */
    @Test
    public void testConcurrentSnapshotRejected() throws Exception {
        CountDownLatch firstHoldsLock = new CountDownLatch(1);
        CountDownLatch firstDone = new CountDownLatch(1);
        AtomicBoolean secondFailed = new AtomicBoolean(false);

        Thread first = new Thread(() -> {
            try {
                quiesceForSnapshot(() -> {
                    firstHoldsLock.countDown();
                    Thread.sleep(2_000); // hold lock for 2 seconds
                });
            } catch (Exception e) {
                // unexpected
            } finally {
                firstDone.countDown();
            }
        });

        // Second snapshot uses a 1-second timeout — shorter than first's hold time
        Thread second = new Thread(() -> {
            try {
                firstHoldsLock.await(5, TimeUnit.SECONDS);
                // Use a short-timeout variant directly (simulates cloud_backup_quiesce_timeout=1)
                boolean acquired = lock.writeLock().tryLock(1, TimeUnit.SECONDS);
                if (!acquired) {
                    secondFailed.set(true);
                } else {
                    lock.writeLock().unlock(); // should not reach here
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        first.start();
        second.start();
        first.join(10_000);
        second.join(5_000);

        Assertions.assertTrue(secondFailed.get(),
                "Second concurrent snapshot must time out");
    }
}
