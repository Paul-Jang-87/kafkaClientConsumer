package consumer;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity; 
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
@Slf4j
@RestController 
public class Controller {
	
	private static final Logger errorLogger = LoggerFactory.getLogger("ErrorLogger");
	
	@GetMapping("/gethc")
	public Mono<ResponseEntity<String>> gealthCheck() throws Exception {
		return Mono.just(ResponseEntity.ok("TEST RESPONSE"));                  
	}
	
	@GetMapping("/apim-gw")
	public Mono<ResponseEntity<String>> getHealthCheckAPIM() throws Exception {
		return Mono.just(ResponseEntity.ok("TEST RESPONSE"));
	}
	
	@GetMapping("/kafka-gw")
	public Mono<ResponseEntity<String>> getHealthCheckKafka() throws Exception {
		return Mono.just(ResponseEntity.ok("TEST RESPONSE"));
	}
	
	/**
	 * [EKS] POD LivenessProbe 헬스체크
	 */
	private final Instant started = Instant.now();
	
    @GetMapping("/healthz")
    public ResponseEntity<String> healthCheck() {
        Duration duration = Duration.between(started, Instant.now());
        if (duration.getSeconds() > 10) {
            return ResponseEntity.status(500).body("error: " + duration.getSeconds());
        } else {
            return ResponseEntity.ok("ok");
        }
    }
    
    
    @Scheduled(cron = "0 3 0 * * *") // 매일 12:03에 실행
	public void startlogs() {
		
		Date today = new Date();

		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy년 MM월 dd일 HH시 mm분 ss초");
		log.info("{}, consumer 로그 시작",dateFormat.format(today).toString());
		log.error("{}, consumer_error 로그 시작",dateFormat.format(today).toString());	
		errorLogger.error("{}, consumer_error 로그 시작",dateFormat.format(today).toString());
    }
}
