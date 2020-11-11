package demo.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Kafka Consumer 订阅主题，指定 partition seek
 *
 * @author yulinying
 * @since 2020/11/11
 */
@Slf4j
public class KafkaConsumerSeekSample {
    
    
    public static void main(String[] args) {
        
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("group.id", "my-consumer");
        properties.put("auto.commit.offset", "true");  
        
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList("test-topic"));
    }
    
    
    
    /**
     * 消费记录
     */
    public static void consumerRecord(KafkaConsumer<String, String> consumer){
        // 轮询查询
        try{
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
                log.info("listen record count:{}", records.count());
                records.forEach(record -> log.info("consumer record: topic={}, value={}, offset={}, partition={}",
                        record.topic(), record.value(), record.offset(), record.partition()));
            }
        } finally{
            consumer.commitSync();
            consumer.close();
        }
    }
    
}
