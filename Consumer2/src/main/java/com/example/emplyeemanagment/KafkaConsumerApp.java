package com.example.emplyeemanagment;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;

import com.example.emplyeemanagment.entity.EmployeeEntity;
import com.example.emplyeemanagment.service.EmployeeService;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Component
public class KafkaConsumerApp {

	private static List<String> topicNames = List.of("firsttopic", "secondtopic", "thirdtopic", "forthtopic",
			"fifthtopic", "sixthtopic");
	private static int numberOfConsumers = 6;

	public KafkaConsumerApp() {
		
	}
	
	public void startConsuming() {
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
                            .doOnNext(records -> processRecords(records, consumer)))
                    .blockLast(); // You can also use .subscribe() instead of .blockLast()

        } finally {
            consumers.forEach(consumer -> {
                try {
                    consumer.commitSync();
                } finally {
                    consumer.close();
                }
            });
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

	private void processRecords(ConsumerRecords<String, String> records, Consumer<String, String> consumer) {
        Flux.fromIterable(records)
                .doOnNext(record -> {
                    String infoString = String.format("topic=%s, partition=%d, offset=%d, key=%s, value=%s\n",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    System.out.println(infoString);
                })
                .flatMap(record -> webClientConnect())
                .subscribe(); // Ensure to subscribe to trigger the processing

        consumer.commitAsync();
    }
	
	private Mono<EmployeeEntity> webClientConnect() {
        WebClient webClient = WebClient.create("http://localhost:8080");
        String endpointUrl = "/employee";

        EmployeeEntity testEmployee = new EmployeeEntity();
        testEmployee.setName("John Doe");
        testEmployee.setGender("Male");
        testEmployee.setAddress("123 Main St");

        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            Date dateOfBirth = dateFormat.parse("1990-01-01");
            testEmployee.setDateOfBirth(dateOfBirth);
        } catch (ParseException e) {
            return Mono.error(e);
        }

        return webClient.post()
                .uri(endpointUrl)
                .contentType(MediaType.APPLICATION_JSON)
                .body(BodyInserters.fromValue(testEmployee))
                .retrieve()
                .bodyToMono(EmployeeEntity.class);
    }
}
