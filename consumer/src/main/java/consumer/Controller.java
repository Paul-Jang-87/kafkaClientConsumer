package consumer;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import reactor.core.publisher.Mono;

@RestController 
public class Controller {
	
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

}
