package com.example.Consumer2;

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

@SpringBootApplication
public class SecondversionApplication {

    private static List<String> topicNames = List.of("firsttopic", "secondtopic", "thirdtopic"); 
    private static int numberOfConsumers = 3; // Change the number of consumers as needed

    public static void main(String[] args) {
        SpringApplication.run(SecondversionApplication.class, args);

        List<Consumer<String, String>> consumers = new ArrayList<>();

        try {
            for (int i = 0; i < numberOfConsumers; i++) {
                Properties consumerProps = createConsumerProperties("group" + i);
                Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps);

                consumer.subscribe(Collections.singletonList(topicNames.get(i)));
                
                consumers.add(consumer);

            }

            while (true) {
                for (Consumer<String, String> consumer : consumers) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                    processRecords(records);
                    consumer.commitAsync();
                }
            }

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

    private static void processRecords(ConsumerRecords<String, String> records) {
        for (ConsumerRecord<String, String> record : records) {
            String infoString = String.format("topic=%s, partition=%d, offset=%d, key=%s, value=%s\n",
                    record.topic(), record.partition(), record.offset(), record.key(), record.value());
            System.out.println(infoString);
        }
    }
}