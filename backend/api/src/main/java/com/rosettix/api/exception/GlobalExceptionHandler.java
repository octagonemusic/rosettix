package com.rosettix.api.exception;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Global exception handler for the Rosettix API
 */
@RestControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    @ExceptionHandler(QueryException.class)
    public ResponseEntity<Map<String, Object>> handleQueryException(
        QueryException ex
    ) {
        log.error("Query exception occurred: {}", ex.toString(), ex);

        Map<String, Object> response = new HashMap<>();
        response.put("timestamp", LocalDateTime.now());
        response.put("error", "Query Error");
        response.put("message", ex.getMessage());
        response.put("errorType", ex.getErrorType().toString());

        if (ex.getStrategyName() != null) {
            response.put("strategy", ex.getStrategyName());
        }

        HttpStatus status = mapErrorTypeToHttpStatus(ex.getErrorType());
        response.put("status", status.value());

        return ResponseEntity.status(status).body(response);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> handleIllegalArgumentException(
        IllegalArgumentException ex
    ) {
        log.error("Illegal argument exception: {}", ex.getMessage(), ex);

        Map<String, Object> response = new HashMap<>();
        response.put("timestamp", LocalDateTime.now());
        response.put("error", "Invalid Request");
        response.put("message", ex.getMessage());
        response.put("status", HttpStatus.BAD_REQUEST.value());

        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
    }

    @ExceptionHandler(SecurityException.class)
    public ResponseEntity<Map<String, Object>> handleSecurityException(
        SecurityException ex
    ) {
        log.error("Security exception: {}", ex.getMessage(), ex);

        Map<String, Object> response = new HashMap<>();
        response.put("timestamp", LocalDateTime.now());
        response.put("error", "Security Error");
        response.put("message", ex.getMessage());
        response.put("status", HttpStatus.FORBIDDEN.value());

        return ResponseEntity.status(HttpStatus.FORBIDDEN).body(response);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleGenericException(
        Exception ex
    ) {
        log.error("Unexpected exception occurred: {}", ex.getMessage(), ex);

        Map<String, Object> response = new HashMap<>();
        response.put("timestamp", LocalDateTime.now());
        response.put("error", "Internal Server Error");
        response.put(
            "message",
            "An unexpected error occurred while processing your request"
        );
        response.put("status", HttpStatus.INTERNAL_SERVER_ERROR.value());

        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(
            response
        );
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> handleValidationException(
        MethodArgumentNotValidException ex
    ) {
        log.error("Validation error occurred: {}", ex.getMessage());

        Map<String, Object> response = new HashMap<>();
        response.put("timestamp", LocalDateTime.now());
        response.put("error", "Validation Error");
        response.put("status", HttpStatus.BAD_REQUEST.value());

        // Collect all field validation errors
        Map<String, String> fieldErrors = ex
            .getBindingResult()
            .getFieldErrors()
            .stream()
            .collect(
                Collectors.toMap(
                    FieldError::getField,
                    FieldError::getDefaultMessage,
                    (existing, replacement) -> existing // Keep first error if multiple for same field
                )
            );

        response.put("fieldErrors", fieldErrors);
        response.put(
            "message",
            "Request validation failed: " +
                fieldErrors.values().stream().collect(Collectors.joining(", "))
        );

        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
    }

    private HttpStatus mapErrorTypeToHttpStatus(
        QueryException.ErrorType errorType
    ) {
        switch (errorType) {
            case STRATEGY_NOT_FOUND:
                return HttpStatus.BAD_REQUEST;
            case UNSAFE_QUERY:
                return HttpStatus.FORBIDDEN;
            case EXECUTION_ERROR:
            case PARSING_ERROR:
                return HttpStatus.BAD_REQUEST;
            case LLM_ERROR:
            case DATABASE_CONNECTION_ERROR:
                return HttpStatus.SERVICE_UNAVAILABLE;
            default:
                return HttpStatus.INTERNAL_SERVER_ERROR;
        }
    }
}