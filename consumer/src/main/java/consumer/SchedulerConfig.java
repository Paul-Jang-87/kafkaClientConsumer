package consumer;

import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

@Configuration
public class SchedulerConfig implements SchedulingConfigurer {
	private final int POOL_SIZE = 10;
	
	@Override
	public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
		// Thread Pool 설정
		ThreadPoolTaskScheduler threadPool = new ThreadPoolTaskScheduler(); 
		
		threadPool.setPoolSize(POOL_SIZE);
		threadPool.setThreadNamePrefix("consumer-scheduling");
		threadPool.initialize();
        
		taskRegistrar.setTaskScheduler(threadPool);
	}
	
}
