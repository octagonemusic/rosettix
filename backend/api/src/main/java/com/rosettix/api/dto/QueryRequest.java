package com.rosettix.api.dto;

import com.rosettix.api.validation.ValidDatabaseStrategy;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import lombok.Data;

@Data
public class QueryRequest {

    @NotBlank(message = "Question cannot be empty")
    @Size(
        min = 3,
        max = 1000,
        message = "Question must be between 3 and 1000 characters"
    )
    private String question;

    @ValidDatabaseStrategy(message = "Invalid database strategy")
    private String database; // Will be set to default strategy if null
}
