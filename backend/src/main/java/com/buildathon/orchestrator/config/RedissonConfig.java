package com.buildathon.orchestrator.config;

import com.buildathon.orchestrator.lock.LockManager;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Raw Redisson client — deliberately not the Spring Boot starter, which
 * targets a different Boot generation. Locks and rate limiters use this
 * client; caching/pub-sub use Spring Data Redis.
 */
@Configuration
public class RedissonConfig {

    @Bean(destroyMethod = "shutdown")
    public RedissonClient redissonClient(
            @Value("${spring.data.redis.host:localhost}") String host,
            @Value("${spring.data.redis.port:6379}") int port) {
        Config config = new Config();
        config.useSingleServer()
                .setAddress("redis://" + host + ":" + port)
                // Render's free Redis tier allows only a handful of client
                // connections; Redisson's default pool (~24) exhausts it.
                .setConnectionPoolSize(4)
                .setConnectionMinimumIdleSize(1);
        return Redisson.create(config);
    }

    @Bean
    public LockManager lockManager(RedissonClient redissonClient, OrchestratorProperties properties) {
        return new LockManager(redissonClient, properties.lock().leaseTimeMs());
    }
}
