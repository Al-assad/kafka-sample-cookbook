package site.assad.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import site.assad.demo.message.SampleTopic;

@SpringBootApplication
@EnableBinding({SampleTopic.class,SampleTopic2.class})
public class Application {

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

}
