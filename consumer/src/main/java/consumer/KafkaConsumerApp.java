package consumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;

import consumer.mappingTopicGroup.MappingTopicGroup;
import reactor.core.publisher.Flux;

@Component
public class KafkaConsumerApp {

	private static List<String> topicNames = List.of("firsttopic", "secondtopic", "thirdtopic", "forthtopic",
			"fifthtopic", "sixthtopic",
			"callbotfirst","callbotsecond","callbotthird","callbotforth","callbotfifth","callbotsixth"
			);
	private static int numberOfConsumers = topicNames.size();

	public KafkaConsumerApp() {  

	}

	public void startConsuming() {   

		List<Consumer<String, String>> consumers = new ArrayList<>();
		
		try {
			MappingTopicGroup mappingData = new MappingTopicGroup();
			
			for (int i = 0; i < numberOfConsumers; i++) {
				Properties consumerProps = createConsumerProperties(mappingData.getGroupBytopic(topicNames.get(i)));
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
		Flux.fromIterable(records).flatMap(record -> PrintingMsg(record.value())).subscribe();

		consumer.commitAsync();
	}

	public Flux<String> processKafkaMessage(String msg) {
		WebClient webClient = WebClient.builder().baseUrl("http://localhost:8083").build();

		String endpointUrl = "/api/receive-message";

		return webClient.post().uri(endpointUrl).body(BodyInserters.fromValue(msg)).retrieve().bodyToMono(String.class)
				.flux(); 
	}

	public  Flux<String> PrintingMsg(String msg) {
	
	 System.out.println(msg);
	 
	 return Flux.just(msg);
		
	}

}
