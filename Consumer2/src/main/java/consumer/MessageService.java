package consumer;

import org.springframework.stereotype.Service;

import consumer.repository.KafkaMessageRepository;

@Service
public class MessageService {

    private final KafkaMessageRepository messageRepository;

    public MessageService(KafkaMessageRepository messageRepository) {
        this.messageRepository = messageRepository;
    }

    public void saveMessage(String key, String value) {
    	try {
    		System.out.println("===성공==="+key+" ,"+value);
            KafkaMessageEntity messageEntity = new KafkaMessageEntity();
            messageEntity.setKey(key);
            messageEntity.setValue(value + "1");  // Adjust based on your requirements
            System.out.println("키 벨류"+messageEntity.getKey()+" ,키 벨류"+messageEntity.getValue());
            messageRepository.save(messageEntity);
            System.out.println("===????성공????===");
            
        } catch (Exception e) {
            // Handle the exception, log it, or take appropriate action
            e.printStackTrace();  // Example: Print the stack trace
        }
    }
}