package site.assad.demo;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;

/**
 * @author yulinying
 * @since 2020/11/11
 */
@Configuration
public class KafkaConfig {
    
    
    /**
     * 转换器
     */
    @Bean
    public RecordMessageConverter converter() {
        return new StringJsonMessageConverter();
    }
    
    /**
     * 自动创建新 topic
     */
    @Bean
    public NewTopic topic() {
        return new NewTopic("topic1", 1, (short) 1);
    }
    
    /**
     * 创建 topic1 的 DLT 死信队列
     */
    @Bean
    public NewTopic topicDLT() {
        return new NewTopic("topic1.DLT", 1, (short) 1);
    }
    
    
}
