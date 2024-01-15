package com.example.emplyeemanagment.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.example.emplyeemanagment.entity.KafkaMessageEntity;

@Repository
public interface KafkaMessageRepository extends JpaRepository<KafkaMessageEntity, Long> {
}
