package com.zrivot.enrichment;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * JVM-wide (per task-manager) resource limiter for enrichment API calls.
 *
 * <p>Provides two shared resources:
 * <ol>
 *   <li><b>Concurrency semaphore</b> — limits the total number of in-flight API
 *       requests across <em>all</em> enricher operator instances on the same TM.
 *       This prevents thread-pool exhaustion and protects downstream services from
 *       overload when multiple enrichers run in the same JVM.</li>
 *   <li><b>Cached thread pool</b> — used for scheduling rate-limit delays and
 *       blocking on the semaphore <em>off</em> the Flink task thread, ensuring the
 *       task thread is never blocked.</li>
 * </ol>
 *
 * <p>The singleton is initialised lazily on the first call to
 * {@link #initialize(int)}.  Subsequent calls are no-ops (first-writer-wins).</p>
 */
@Slf4j
public final class SharedEnrichmentExecutor {

    private static volatile ExecutorService executor;
    private static volatile Semaphore concurrencyLimiter;

    private SharedEnrichmentExecutor() { /* utility */ }

    /**
     * Initialises the shared resources if not already done.
     *
     * @param maxConcurrentRequests maximum concurrent API requests per TM
     */
    public static synchronized void initialize(int maxConcurrentRequests) {
        if (executor == null) {
            log.info("Initialising SharedEnrichmentExecutor: maxConcurrentRequests={}",
                    maxConcurrentRequests);
            executor = Executors.newCachedThreadPool(r -> {
                Thread t = new Thread(r, "zrivot-enrichment-worker");
                t.setDaemon(true);
                return t;
            });
            concurrencyLimiter = new Semaphore(maxConcurrentRequests);
        }
    }

    /**
     * Schedules a task with optional rate-limit delay <b>and</b> concurrency control.
     *
     * <ol>
     *   <li>If {@code rateLimitDelayMicros > 0}, the thread sleeps for that duration.</li>
     *   <li>Acquires a concurrency semaphore permit (may block).</li>
     *   <li>Runs {@code task}.  The task <b>must</b> eventually call
     *       {@link #releaseConcurrencyPermit()} — typically in a
     *       {@code whenComplete} callback on the enrichment future.</li>
     * </ol>
     *
     * <p>The entire sequence runs on the shared cached thread pool, so the Flink
     * task thread returns immediately.</p>
     */
    public static void scheduleWithConcurrency(long rateLimitDelayMicros, Runnable task) {
        executor.execute(() -> {
            try {
                if (rateLimitDelayMicros > 0) {
                    TimeUnit.MICROSECONDS.sleep(rateLimitDelayMicros);
                }
                concurrencyLimiter.acquire();
                task.run();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("SharedEnrichmentExecutor interrupted while waiting for permit/delay");
            }
        });
    }

    /**
     * Releases one concurrency permit.  Must be called exactly once per successful
     * {@link #scheduleWithConcurrency} invocation, typically in a
     * {@code whenComplete} handler.
     */
    public static void releaseConcurrencyPermit() {
        if (concurrencyLimiter != null) {
            concurrencyLimiter.release();
        }
    }

    /** Returns the shared executor (for scheduling retries, etc.). */
    public static ExecutorService executor() {
        return executor;
    }
}
