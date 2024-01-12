package consumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import reactor.core.publisher.Flux;

@SpringBootApplication
public class KafkaConsumerApp {

	private static List<String> topicNames = List.of("firsttopic", "secondtopic", "thirdtopic","forthtopic","fifthtopic","sixthtopic");
    private static int numberOfConsumers = 6;

    public static void main(String[] args) {
        SpringApplication.run(KafkaConsumerApp.class, args);

        List<Consumer<String, String>> consumers = new ArrayList<>();

        try {
            for (int i = 0; i < numberOfConsumers; i++) {
                Properties consumerProps = createConsumerProperties("group" + i);
                Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps);

                consumer.subscribe(Collections.singletonList(topicNames.get(i)));
                consumers.add(consumer);
            }

            Flux.fromIterable(consumers)
                    .flatMap(consumer -> Flux.interval(Duration.ofMillis(500))
                            .map(tick -> consumer.poll(Duration.ofMillis(500)))
                            .doOnNext(records -> processRecords(records, consumer))
                            .doOnNext(records -> consumer.commitAsync()))
                    .blockLast(); // This will block the main thread to keep the application running

        } finally {
            for (Consumer<String, String> consumer : consumers) {
                try {
                    consumer.commitSync();
                } finally {
                    consumer.close();
                }
            }
        }
    }

    private static Properties createConsumerProperties(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return props;
    }

    private static void processRecords(ConsumerRecords<String, String> records, Consumer<String, String> consumer) {
        for (ConsumerRecord<String, String> record : records) {
            String infoString = String.format("topic=%s, partition=%d, offset=%d, key=%s, value=%s\n",
                    record.topic(), record.partition(), record.offset(), record.key(), record.value());
            System.out.println(infoString);
        }
    }
}