package com.zrivot.enrichment;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiter;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

/**
 * JVM-wide (per task-manager) resource limiter for enrichment API calls.
 *
 * <p>Provides two shared resources:
 * <ol>
 *   <li><b>Concurrency semaphore</b> — limits the total number of in-flight API
 *       requests across <em>all</em> enricher operator instances on the same TM.
 *       This prevents thread-pool exhaustion and protects downstream services from
 *       overload when multiple enrichers run in the same JVM.</li>
 *   <li><b>Cached thread pool</b> — used for blocking on the rate limiter and
 *       semaphore <em>off</em> the Flink task thread, ensuring the task thread is
 *       never blocked.</li>
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
     * Acquires a rate-limit permit via Flink's {@link RateLimiter}, then acquires
     * a concurrency semaphore permit, and finally runs the task.
     *
     * <ol>
     *   <li>Calls {@code rateLimiter.acquire()} — blocks until a rate-limit token
     *       is available (for {@code GuavaRateLimiter} this uses Guava's smooth
     *       rate limiter; for {@code NoOpRateLimiter} it returns immediately).</li>
     *   <li>Acquires a concurrency semaphore permit (may block).</li>
     *   <li>Runs {@code task}.  The task <b>must</b> eventually call
     *       {@link #releaseConcurrencyPermit()} — typically in a
     *       {@code whenComplete} callback on the enrichment future.</li>
     * </ol>
     *
     * <p>The entire sequence runs on the shared cached thread pool, so the Flink
     * task thread returns immediately.</p>
     */
    public static void executeWithConcurrency(RateLimiter rateLimiter, Runnable task) {
        executor.execute(() -> {
            try {
                rateLimiter.acquire().toCompletableFuture().join();
                concurrencyLimiter.acquire();
                task.run();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("SharedEnrichmentExecutor interrupted while waiting for permit");
            }
        });
    }

    /**
     * Releases one concurrency permit.  Must be called exactly once per successful
     * {@link #executeWithConcurrency} invocation, typically in a
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
