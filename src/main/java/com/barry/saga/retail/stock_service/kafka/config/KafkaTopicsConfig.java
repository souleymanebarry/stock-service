package com.barry.saga.retail.stock_service.kafka.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "spring.kafka")
@Getter
@Setter
public class KafkaTopicsConfig {

    private Topics topics = new Topics();

    @Getter @Setter
    public static class Topics {
        private String stockReserved;
        private String stockRejected;
        private String orderPlaced;
    }
}
