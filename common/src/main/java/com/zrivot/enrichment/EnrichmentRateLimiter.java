package com.zrivot.enrichment;

import lombok.extern.slf4j.Slf4j;

/**
 * A simple token-bucket rate limiter for enrichment API requests.
 *
 * <p>Each call to {@link #reservePermitMicros()} reserves the <b>next</b> available
 * time-slot and returns how many <b>microseconds</b> the caller must wait before
 * issuing the API call.  Returning {@code 0} means the caller may proceed immediately.
 *
 * <p>Thread-safe — all state is protected by a synchronised block.
 *
 * <h3>Usage example (non-blocking)</h3>
 * <pre>{@code
 *   long waitMicros = rateLimiter.reservePermitMicros();
 *   if (waitMicros > 0) {
 *       CompletableFuture.delayedExecutor(waitMicros, TimeUnit.MICROSECONDS, executor)
 *           .execute(() -> callApi());
 *   } else {
 *       callApi();
 *   }
 * }</pre>
 *
 * <p>When {@code permitsPerSecond} is {@code 0} (or negative), the limiter is
 * disabled and {@link #reservePermitMicros()} always returns {@code 0}.</p>
 */
@Slf4j
public class EnrichmentRateLimiter {

    private final int permitsPerSecond;
    private final long intervalNanos;
    private long nextPermitTimeNanos;

    /**
     * @param permitsPerSecond max requests per second; {@code 0} = unlimited
     */
    public EnrichmentRateLimiter(int permitsPerSecond) {
        this.permitsPerSecond = permitsPerSecond;
        this.intervalNanos = permitsPerSecond > 0
                ? 1_000_000_000L / permitsPerSecond
                : 0;
        this.nextPermitTimeNanos = System.nanoTime();
    }

    /** Returns {@code true} when rate limiting is active. */
    public boolean isEnabled() {
        return permitsPerSecond > 0;
    }

    /**
     * Reserves one permit and returns the number of <b>microseconds</b> the caller
     * should wait before making the API call.  {@code 0} means proceed immediately.
     */
    public synchronized long reservePermitMicros() {
        if (permitsPerSecond <= 0) {
            return 0;
        }

        long now = System.nanoTime();

        if (now >= nextPermitTimeNanos) {
            // Slot is available now — reserve it and advance the pointer
            nextPermitTimeNanos = now + intervalNanos;
            return 0;
        }

        // Slot is in the future — the caller must wait
        long waitNanos = nextPermitTimeNanos - now;
        nextPermitTimeNanos += intervalNanos;
        return Math.max(1, waitNanos / 1_000); // nanos → micros, at least 1µs
    }
}
