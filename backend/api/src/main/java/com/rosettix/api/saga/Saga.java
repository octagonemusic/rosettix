package com.rosettix.api.saga;

import lombok.Data;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Represents an entire Saga (multi-step transaction).
 */
@Data
public class Saga {
    private final String sagaId = UUID.randomUUID().toString();
    private final Instant startTime = Instant.now();
    private final List<SagaStep> steps = new ArrayList<>();

    public void addStep(SagaStep step) {
        steps.add(step);
    }
}