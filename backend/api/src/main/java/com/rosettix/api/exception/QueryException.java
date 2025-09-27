package com.rosettix.api.exception;

/**
 * Custom exception for query-related errors in the Rosettix system
 */
public class QueryException extends RuntimeException {

    private final String strategyName;
    private final String query;
    private final ErrorType errorType;

    public enum ErrorType {
        STRATEGY_NOT_FOUND,
        UNSAFE_QUERY,
        EXECUTION_ERROR,
        PARSING_ERROR,
        LLM_ERROR,
        DATABASE_CONNECTION_ERROR,
    }

    public QueryException(String message) {
        super(message);
        this.strategyName = null;
        this.query = null;
        this.errorType = ErrorType.EXECUTION_ERROR;
    }

    public QueryException(String message, Throwable cause) {
        super(message, cause);
        this.strategyName = null;
        this.query = null;
        this.errorType = ErrorType.EXECUTION_ERROR;
    }

    public QueryException(
        String message,
        String strategyName,
        ErrorType errorType
    ) {
        super(message);
        this.strategyName = strategyName;
        this.query = null;
        this.errorType = errorType;
    }

    public QueryException(
        String message,
        String strategyName,
        String query,
        ErrorType errorType
    ) {
        super(message);
        this.strategyName = strategyName;
        this.query = query;
        this.errorType = errorType;
    }

    public QueryException(
        String message,
        String strategyName,
        String query,
        ErrorType errorType,
        Throwable cause
    ) {
        super(message, cause);
        this.strategyName = strategyName;
        this.query = query;
        this.errorType = errorType;
    }

    public String getStrategyName() {
        return strategyName;
    }

    public String getQuery() {
        return query;
    }

    public ErrorType getErrorType() {
        return errorType;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("QueryException{");
        sb.append("errorType=").append(errorType);
        if (strategyName != null) {
            sb.append(", strategy='").append(strategyName).append("'");
        }
        if (query != null) {
            sb
                .append(", query='")
                .append(
                    query.length() > 50 ? query.substring(0, 50) + "..." : query
                )
                .append("'");
        }
        sb.append(", message='").append(getMessage()).append("'");
        sb.append("}");
        return sb.toString();
    }
}
