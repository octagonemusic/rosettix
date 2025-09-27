package com.rosettix.api.validation;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;
import java.lang.annotation.*;

/**
 * Custom validation annotation that dynamically validates database strategy names
 * against the available strategies in the application context.
 */
@Documented
@Constraint(validatedBy = DatabaseStrategyValidator.class)
@Target({ElementType.FIELD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
public @interface ValidDatabaseStrategy {

    String message() default "Invalid database strategy. Available strategies: {availableStrategies}";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};

    /**
     * Whether null values should be considered valid
     */
    boolean allowNull() default true;
}
