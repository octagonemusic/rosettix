package com.rosettix.api.service;

import com.rosettix.api.config.RosettixConfiguration;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class PgvectorSemanticQueryCacheInitializer {

    private final RosettixConfiguration configuration;
    private final PgvectorSemanticQueryCacheRepository repository;

    @PostConstruct
    public void initialize() {
        if (!configuration.getQuery().isSemanticMatchingEnabled()) {
            return;
        }
        if (!configuration.getQuery().isSemanticPgvectorAutoInit()) {
            return;
        }

        try {
            repository.initializeSchema(configuration.getLlm().getEmbeddingDimensions());
            log.info("Initialized pgvector semantic query cache schema");
        } catch (Exception e) {
            log.warn("Unable to initialize pgvector semantic query cache schema: {}", e.getMessage());
        }
    }
}
