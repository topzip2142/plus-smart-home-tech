package ru.yandex.practicum.analyzer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;
import ru.yandex.practicum.analyzer.service.HubEventProcessor;
import ru.yandex.practicum.analyzer.service.SnapshotProcessor;

import java.util.concurrent.TimeUnit;

@Slf4j
@SpringBootApplication
@ConfigurationPropertiesScan
public class Analyzer {

    private static final long SHUTDOWN_TIMEOUT_SECONDS = 15;

    public static void main(String[] args) {
        ConfigurableApplicationContext context =
                SpringApplication.run(Analyzer.class, args);

        HubEventProcessor hubEventProcessor = context.getBean(HubEventProcessor.class);
        SnapshotProcessor snapshotProcessor = context.getBean(SnapshotProcessor.class);

        Thread hubEventsThread = new Thread(hubEventProcessor);
        hubEventsThread.setName("HubEventHandlerThread");

        Thread snapshotThread = new Thread(snapshotProcessor::start);
        snapshotThread.setName("SnapshotHandlerThread");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received, stopping processors");

            hubEventProcessor.stop();
            snapshotProcessor.stop();

            joinQuietly(hubEventsThread, "HubEventHandlerThread");
            joinQuietly(snapshotThread, "SnapshotHandlerThread");

            log.info("Processors stopped");
        }, "AnalyzerShutdownHook"));

        hubEventsThread.start();
        snapshotThread.start();
    }

    private static void joinQuietly(Thread thread, String name) {
        try {
            thread.join(TimeUnit.SECONDS.toMillis(SHUTDOWN_TIMEOUT_SECONDS));
            if (thread.isAlive()) {
                log.warn("Thread {} did not stop within {}s, interrupting",
                        name, SHUTDOWN_TIMEOUT_SECONDS);
                thread.interrupt();
                thread.join(TimeUnit.SECONDS.toMillis(5));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while waiting for thread {} to stop", name, e);
        }
    }
}