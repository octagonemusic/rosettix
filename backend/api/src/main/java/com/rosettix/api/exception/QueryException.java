package com.rosettix.api.exception;

import lombok.Getter;

/**
 * Unified exception for all query and orchestration errors.
 */
@Getter
public class QueryException extends RuntimeException {

    private final String strategyName;
    private final String generatedQuery;
    private final ErrorType errorType;

    // ============================================================
    // ENUM DEFINITIONS
    // ============================================================
    public enum ErrorType {
        STRATEGY_NOT_FOUND,
        UNSAFE_QUERY,
        PARSING_ERROR,
        DATABASE_CONNECTION_ERROR,
        EXECUTION_ERROR,
        LLM_ERROR,
        UNSUPPORTED_OPERATION
    }

    // ============================================================
    // CONSTRUCTORS
    // ============================================================

    // Basic: message + strategy + type
    public QueryException(String message, String strategyName, ErrorType errorType) {
        super(message);
        this.strategyName = strategyName;
        this.generatedQuery = null;
        this.errorType = errorType;
    }

    // With generated query (for LLM or executed SQL)
    public QueryException(String message, String strategyName, String generatedQuery, ErrorType errorType) {
        super(message);
        this.strategyName = strategyName;
        this.generatedQuery = generatedQuery;
        this.errorType = errorType;
    }

    // With cause (Exception chaining)
    public QueryException(String message, String strategyName, ErrorType errorType, Throwable cause) {
        super(message, cause);
        this.strategyName = strategyName;
        this.generatedQuery = null;
        this.errorType = errorType;
    }

    // With generated query + cause
    public QueryException(String message, String strategyName, String generatedQuery, ErrorType errorType, Throwable cause) {
        super(message, cause);
        this.strategyName = strategyName;
        this.generatedQuery = generatedQuery;
        this.errorType = errorType;
    }
}