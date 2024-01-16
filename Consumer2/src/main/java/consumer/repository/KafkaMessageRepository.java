package consumer.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import consumer.KafkaMessageEntity;

@Repository
public interface KafkaMessageRepository extends JpaRepository<KafkaMessageEntity, Long> {
}
