package site.assad.demo;


import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Kafka Consumer 消费信息
 *
 * @author yulinying
 * @since 2020/11/11
 */
@Component
@Slf4j
public class KafkaConsumer {
    
    /**
     * 监听 topic1
     */
    @KafkaListener(groupId = "group1", topics = "topic1")
    public void listenTopic1(Foo foo){
        log.info("receive: {}", foo.toString());
    }
    
    
    /**
     * 监听 topic1，获取其他信息
     */
    @KafkaListener(groupId = "group1", topics = "topic2")
    public void listenTopic2(@Payload Foo foo,
                             @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
                             @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
                             @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                             @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long ts){
        log.info("receive: key={}, partition={}, topic={}, timestamp={}", key, partition, topic, ts);
        log.info("payload={}", foo.toString());
    }






}
