package com.rosettix.api.config;

import com.google.genai.Client;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class GeminiConfig {

    @Bean
    public Client geminiClient() {
        // This uses the new builder to create the client, as per the documentation
        return new Client();
    }
}
