package com.zrivot.enrichment;

import com.zrivot.config.EnricherConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates {@link Enricher} instances from configuration using reflection.
 * Keeps the pipeline agnostic to concrete enricher implementations.
 */
public final class EnricherFactory {

    private static final Logger LOG = LoggerFactory.getLogger(EnricherFactory.class);

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
            LOG.info("Created enricher '{}' from class {}", config.getName(), config.getClassName());
            return enricher;
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Failed to create enricher: " + config.getName(), e);
        }
    }
}
