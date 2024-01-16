package consumer;



import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "mt_kafka")
public class KafkaMessageEntity {

    @Id 
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "kafka_id")
    private Long id;

    @Column(name = "key")
    private String key;
    
    @Column(name = "value")
    private String value;

    public KafkaMessageEntity() {
	}

	public KafkaMessageEntity(Long id, String key, String value) {
		this.id = id;
		this.key = key;
		this.value = value;
		
	}
    
    public void setKey(String key) {
		this.key = key;
	}
    
    public void setValue(String value) {
		this.value = value;
	}
    
    public String getKey() {
		return key;
	}
    
    public String getValue() {
		return value;
	}
}
