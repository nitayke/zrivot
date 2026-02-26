package com.zrivot.enrichment;

import com.zrivot.config.EnricherConfig;
import lombok.extern.slf4j.Slf4j;

/**
 * Creates {@link Enricher} instances from configuration using reflection.
 * Keeps the pipeline agnostic to concrete enricher implementations.
 */
@Slf4j
public final class EnricherFactory {

    private EnricherFactory() {
        // utility class
    }

    /**
     * Instantiates an enricher from the class name specified in the config,
     * then initialises it.
     *
     * @param config the enricher configuration
     * @return an initialised enricher instance
     */
    public static Enricher create(EnricherConfig config) {
        try {
            Class<?> clazz = Class.forName(config.getClassName());
            if (!Enricher.class.isAssignableFrom(clazz)) {
                throw new IllegalArgumentException(
                        "Class " + config.getClassName() + " does not implement Enricher");
            }
            Enricher enricher = (Enricher) clazz.getDeclaredConstructor().newInstance();
            enricher.init(config);
            log.info("Created enricher '{}' from class {}", config.getName(), config.getClassName());
            return enricher;
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Failed to create enricher: " + config.getName(), e);
        }
    }
}
