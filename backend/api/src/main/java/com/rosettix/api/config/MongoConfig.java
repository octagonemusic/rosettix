package com.rosettix.api.config;

import org.springframework.context.annotation.Configuration;

/**
 * MongoDB configuration - relies on Spring Boot auto-configuration
 * Connection details are configured via application.properties:
 * - spring.data.mongodb.uri
 * - spring.data.mongodb.database
 */
@Configuration
public class MongoConfig {
    // Spring Boot will auto-configure MongoTemplate bean based on properties
    // No manual bean creation needed - this makes it fully configurable
}
