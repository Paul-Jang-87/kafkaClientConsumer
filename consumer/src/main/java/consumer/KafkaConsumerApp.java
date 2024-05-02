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
import org.springframework.web.bind.annotation.GetMapping;
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

	private static List<String> topicNames = List.of("from_cscallbot_cmpnhmitem_message",
			"from_cscallbot_cmpnmblitem_message","from_ucrm_cticablcntrtsms_message",
			"from_ucrm_ctiwrlscntrtsms_message");
	private static int numberOftopics = topicNames.size();
	private static String CONSUMER_IP = "";
	private static String CONSUMER_SASL = "";
	private static String CONSUMER_PROTOCAL = "";
	private static String CONSUMER_MECHANISM = "";

	public KafkaConsumerApp() {

	}

	@Value("${consumer.ip}")
	private String ip;
	@Value("${consumer.sasl}")
	private String sasl;
	@Value("${consumer.protocal}")
	private String protocal;
	@Value("${consumer.mechanism}")
	private String mechanism;

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

			for (int i = 0; i < numberOftopics; i++) {
				Properties consumerProps = createConsumerProperties(mappingData.getGroupBytopic(topicNames.get(i)));
				Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
				consumer.subscribe(Collections.singletonList(topicNames.get(i)));
				consumers.add(consumer);
			}
			
			Flux.fromIterable(consumers)
			.flatMap(consumer -> Flux.interval(Duration.ofMillis(800))
					.map(tick -> consumer.poll(Duration.ofMillis(800)))
					.doOnNext(records -> processRecords(records, consumer)))
			.blockLast(); 
			
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

		String saslJassConfig = CONSUMER_SASL;

		log.info("IP Address : {}", CONSUMER_IP);
		log.info("authentication info : {}", saslJassConfig);
		log.info("protocal : {}", CONSUMER_PROTOCAL);
		log.info("mechanism : {}", CONSUMER_MECHANISM);

		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CONSUMER_IP);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

		// sasl 설정 파트
		props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, CONSUMER_PROTOCAL);
		props.put(SaslConfigs.SASL_MECHANISM, CONSUMER_MECHANISM);
		props.put(SaslConfigs.SASL_JAAS_CONFIG, saslJassConfig);

		log.info("프롭 : {}", props.toString());

		return props;
	}

	private void processRecords(ConsumerRecords<String, String> records, Consumer<String, String> consumer) {
		Flux.fromIterable(records).flatMap(record -> processKafkaMessage(record, consumer)).subscribe();

		consumer.commitAsync();
	}


	public Flux<String> processKafkaMessage(ConsumerRecord<String, String> record, Consumer<String, String> consumer) {

		log.info("====== processKafkaMessage ======");
		String topic = record.topic();
		String msg = record.value();

		WebClient webClient = WebClient.builder().baseUrl("http://localhost:8083").build();
		String endpointUrl = "";

		switch (topic) {

		case "from_ucrm_cticablcntrtsms_message":

			endpointUrl = "/saveucrmdata";
			break;

		case "from_ucrm_ctiwrlscntrtsms_message":

			endpointUrl = "/saveucrmdata";
			break;

		case "from_cscallbot_cmpnhmitem_message":

			endpointUrl = "/contactlt/callbothome";
			break;

		case "from_cscallbot_cmpnmblitem_message":

			endpointUrl = "/contactlt/callbotmobile";
			break;

		default:
			// Default case if the topic is not handled
			log.info("Unhandled topic: {}", topic);
			return Flux.empty();
		}

		log.info("API_EndPoint : {}", endpointUrl);
		log.info("{} 토픽에서 컨슈머가 받은 메시지 : {}", topic, msg);

		return webClient.post().uri(endpointUrl).body(BodyInserters.fromValue(msg)).retrieve().bodyToMono(String.class)
				.map(response -> {
					log.info("Response from G.C API: {}", response);
					return response;
				}).flux();
	}

	@GetMapping("/gethc")
	public String gealthCheck() throws Exception {
		return "TEST RESPONSE";
	}

}
