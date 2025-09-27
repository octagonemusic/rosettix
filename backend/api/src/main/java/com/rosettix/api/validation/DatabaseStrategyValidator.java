package com.rosettix.api.validation;

import com.rosettix.api.strategy.QueryStrategy;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;

/**
 * Validator implementation that dynamically checks if a database strategy name
 * is valid against the currently available strategies in the application context.
 */
@Component
public class DatabaseStrategyValidator implements ConstraintValidator<ValidDatabaseStrategy, String> {

    @Autowired
    private Map<String, QueryStrategy> strategies;

    private boolean allowNull;

    @Override
    public void initialize(ValidDatabaseStrategy constraintAnnotation) {
        this.allowNull = constraintAnnotation.allowNull();
    }

    @Override
    public boolean isValid(String value, ConstraintValidatorContext context) {
        // Handle null values based on allowNull setting
        if (value == null) {
            return allowNull;
        }

        // Check if the strategy exists
        boolean isValid = strategies.containsKey(value.trim());

        if (!isValid) {
            // Customize the error message to include available strategies
            context.disableDefaultConstraintViolation();

            Set<String> availableStrategies = strategies.keySet();
            String errorMessage = String.format(
                "Invalid database strategy '%s'. Available strategies: %s",
                value,
                availableStrategies
            );

            context.buildConstraintViolationWithTemplate(errorMessage)
                   .addConstraintViolation();
        }

        return isValid;
    }
}
