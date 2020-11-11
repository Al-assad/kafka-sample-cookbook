package site.assad.demo;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class Application {

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}
	
	/**
	 * 自动创建新 topic
	 */
	@Bean
	public NewTopic topic() {
		return new NewTopic("topic1", 1, (short) 1);
	}

}
