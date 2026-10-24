package ru.yandex.practicum.analyzer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;
import ru.yandex.practicum.analyzer.service.HubEventProcessor;
import ru.yandex.practicum.analyzer.service.SnapshotProcessor;

@SpringBootApplication
@ConfigurationPropertiesScan
public class Analyzer {

    public static void main(String[] args) {
        ConfigurableApplicationContext context =
                SpringApplication.run(Analyzer.class, args);

        HubEventProcessor hubEventProcessor = context.getBean(HubEventProcessor.class);
        SnapshotProcessor snapshotProcessor = context.getBean(SnapshotProcessor.class);

        Thread hubEventsThread = new Thread(hubEventProcessor);
        hubEventsThread.setName("HubEventHandlerThread");
        hubEventsThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            hubEventProcessor.stop();
            snapshotProcessor.stop();
        }));

        snapshotProcessor.start();
    }
}
