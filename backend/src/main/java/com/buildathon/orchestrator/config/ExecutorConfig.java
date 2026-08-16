package com.buildathon.orchestrator.config;

import com.buildathon.orchestrator.worker.BashExecutor;
import com.buildathon.orchestrator.worker.DelayExecutor;
import com.buildathon.orchestrator.worker.FailExecutor;
import com.buildathon.orchestrator.worker.HttpExecutor;
import com.buildathon.orchestrator.worker.TaskExecutor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ExecutorConfig {

    @Bean
    public TaskExecutor bashExecutor() {
        return new BashExecutor();
    }

    @Bean
    public TaskExecutor httpExecutor() {
        return new HttpExecutor();
    }

    @Bean
    public TaskExecutor delayExecutor() {
        return new DelayExecutor();
    }

    @Bean
    public TaskExecutor failExecutor() {
        return new FailExecutor();
    }
}
