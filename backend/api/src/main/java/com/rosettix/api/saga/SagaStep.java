package com.rosettix.api.saga;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents a single step in a Saga transaction sequence.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SagaStep {
    private String question;   // natural language query
    private String database;   // e.g., "postgres", "mongodb"
    private String forwardQuery;  // actual generated query
    private String compensationQuery; // rollback query
}