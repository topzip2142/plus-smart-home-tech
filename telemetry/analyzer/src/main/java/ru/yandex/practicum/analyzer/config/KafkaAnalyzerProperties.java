package ru.yandex.practicum.analyzer.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "analyzer.kafka")
public class KafkaAnalyzerProperties {

    private Topics topics = new Topics();
    private Consumer consumer = new Consumer();
    private long pollTimeoutMs = 500;

    @Data
    public static class Topics {
        private String hubs;
        private String snapshots;
    }

    @Data
    public static class Consumer {
        private String hubEventsGroupId;
        private String snapshotsGroupId;
    }
}
