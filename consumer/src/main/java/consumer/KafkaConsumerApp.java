package consumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;

import consumer.mappingTopicGroup.MappingTopicGroup;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;

@Slf4j
@Component
@PropertySource("classpath:application.properties")
public class KafkaConsumerApp {

	private static List<String> topicNames = List.of("from_ucrm_citcablcntrtsms_message",
			"from_ucrm_citwrlscntrtsms_message", "from_cscallbot_cmpnhmitem_message",
			"from_cscallbot_cmpnmblitem_message");
//	private static List<String> topicNames = List.of("thirdtopic", "forthtopic");
	private static int numberOfConsumers = topicNames.size();
	private static String CONSUMER_IP = "";
	private static String CONSUMER_SASL = "";
	private static String CONSUMER_PROTOCAL = "";
	private static String CONSUMER_MECHANISM = "";

	public KafkaConsumerApp() {

	}
	
	@Value("${consumer.ip}")
	private  String ip;
	@Value("${consumer.sasl}")
	private  String sasl;
	@Value("${consumer.protocal}")
	private  String protocal;
	@Value("${consumer.mechanism}")
	private  String mechanism;
	
	@PostConstruct
    public void init() {
		CONSUMER_IP = ip;
		CONSUMER_SASL = sasl;
		CONSUMER_PROTOCAL = protocal;
		CONSUMER_MECHANISM = mechanism;
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

		// SASL configuration part
//		String saslJassConfig = "org.apache.kafka.common.security.scram.ScramLoginModule required " 
//		+ "username=\""
//		+ "clcc_app" // SASL ID
//		+ "\" "
//		+ "password=\"" 
//		+ "UFw6ql7sbNUofJHu" // SASL PASSWORD
//		+ "\";"
//		;
		
		String saslJassConfig = CONSUMER_SASL;
		
		log.info("verions 0.1");
		
		log.info("IP Address : {}",CONSUMER_IP);
		log.info("authentication info : {}",saslJassConfig);
		log.info("protocal : {}",CONSUMER_PROTOCAL);
		log.info("mechanism : {}",CONSUMER_MECHANISM);
		
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CONSUMER_IP);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

		// sasl 설정 파트
		props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, CONSUMER_PROTOCAL);
		props.put(SaslConfigs.SASL_MECHANISM, CONSUMER_MECHANISM);
		props.put(SaslConfigs.SASL_JAAS_CONFIG, saslJassConfig);
		
		log.info("프롭 : {}",   props.toString() );
		
		return props;
	}

	private void processRecords(ConsumerRecords<String, String> records, Consumer<String, String> consumer) {
		Flux.fromIterable(records).flatMap(record -> processKafkaMessage(record, consumer)).subscribe();

		consumer.commitAsync();
	}

//	public Flux<String> processKafkaMessage(String msg) {
//		WebClient webClient = WebClient.builder().baseUrl("http://localhost:8083").build();
//
//		String endpointUrl = "/gcapi/post/thirdtopic";
//
//		return webClient.post().uri(endpointUrl).body(BodyInserters.fromValue(msg)).retrieve().bodyToMono(String.class)
//				.flux(); 
//	}

	public Flux<String> processKafkaMessage(ConsumerRecord<String, String> record, Consumer<String, String> consumer) {

		log.info("====== processKafkaMessage ======");
		String topic = record.topic();
		String msg = record.value();

		WebClient webClient = WebClient.builder().baseUrl("http://localhost:8083").build();
		String endpointUrl = "";
		switch (topic) {

		case "from_ucrm_citcablcntrtsms_message":

			endpointUrl = "/gcapi/post/thirdtopic";
			log.info("API_EndPoint : {}", endpointUrl);
			log.info("{} 토픽에서 컨슈머가 받은 메시지 : {}", topic, msg);
			return webClient.post().uri(endpointUrl).body(BodyInserters.fromValue(msg)).retrieve()
					.bodyToMono(String.class).flux();

		case "from_ucrm_citwrlscntrtsms_message":

			endpointUrl = "/gcapi/post/thirdtopic";
			log.info("API_EndPoint : {}", endpointUrl);
			log.info("{} 토픽에서 컨슈머가 받은 메시지 : {}", topic, msg);
			return webClient.post().uri(endpointUrl).body(BodyInserters.fromValue(msg)).retrieve()
					.bodyToMono(String.class).flux();

		case "from_cscallbot_cmpnhmitem_message":

			endpointUrl = "/gcapi/post/forthtopic";
			log.info("API_EndPoint : {}", endpointUrl);
			log.info("{} 토픽에서 컨슈머가 받은 메시지 : {}", topic, msg);
			return webClient.post().uri(endpointUrl).body(BodyInserters.fromValue(msg)).retrieve()
					.bodyToMono(String.class).flux();

		case "from_cscallbot_cmpnmblitem_message":

			endpointUrl = "/gcapi/post/forthtopic";
			log.info("API_EndPoint : {}", endpointUrl);
			log.info("{} 토픽에서 컨슈머가 받은 메시지 : {}", topic, msg);
			return webClient.post().uri(endpointUrl).body(BodyInserters.fromValue(msg)).retrieve()
					.bodyToMono(String.class).flux();

		default:
			// Default case if the topic is not handled
			return Flux.empty();
		}

	}

}
