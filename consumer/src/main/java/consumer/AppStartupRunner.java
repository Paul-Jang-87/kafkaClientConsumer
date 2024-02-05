package consumer;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
public class AppStartupRunner implements ApplicationRunner {

    private final KafkaConsumerApp kafkaConsumerApp;

    public AppStartupRunner(KafkaConsumerApp kafkaConsumerApp) {
        this.kafkaConsumerApp = kafkaConsumerApp;
    }

    @Override
    public void run(ApplicationArguments args) {
        // Logic to run after the application has started
        kafkaConsumerApp.startConsuming();
    }
}

