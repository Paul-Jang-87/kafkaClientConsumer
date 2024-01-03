package com.example.Consumer2;

import java.time.Duration;
import java.util.Collections;
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
	
	private static String topicName = "firsttopic";

	public static void main(String[] args) {
		
		System.out.println("1");
		
		SpringApplication.run(SecondversionApplication.class, args);
		
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "lguplus");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		
		Consumer<String, String> consumer = new KafkaConsumer<>(props);
		
		consumer.subscribe(Collections.singletonList(topicName));
		
		try {
			
			
			while(true) {
				
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
				
				for(ConsumerRecord<String, String> record : records) {
					
					String infoString = String.format("topic=%s, partition=%d, offset=%d, key=%s, value=%s\n",
							record.topic(),record.partition(),record.offset(),record.key(),record.value());
					System.out.println(infoString);
					
				}
				consumer.commitAsync();
				System.out.println("2");
			}
			
			
		} finally {
			
			try {
				consumer.commitSync();
			} finally {
				consumer.close();
			}
		}
		
	}

}
