package ru.yandex.practicum.aggregator.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "aggregator.kafka")
public class KafkaProperties {

    private Topics topics = new Topics();
    private long pollTimeoutMs = 500;
    private Consumer consumer = new Consumer();

    @Data
    public static class Topics {
        private String sensors;
        private String snapshots;
    }

    @Data
    public static class Consumer {
        private String groupId;
    }
}
