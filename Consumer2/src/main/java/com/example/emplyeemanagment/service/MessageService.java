package com.example.emplyeemanagment.service;

import org.springframework.stereotype.Service;

import com.example.emplyeemanagment.entity.KafkaMessageEntity;
import com.example.emplyeemanagment.repository.KafkaMessageRepository;

@Service
public class MessageService {

    private final KafkaMessageRepository messageRepository;

    public MessageService(KafkaMessageRepository messageRepository) {
        this.messageRepository = messageRepository;
    }

    public void saveMessage(String key, String value) {
        KafkaMessageEntity messageEntity = new KafkaMessageEntity();
        messageEntity.setKey(key);
        messageEntity.setValue(value);
        messageRepository.save(messageEntity);
    }
}