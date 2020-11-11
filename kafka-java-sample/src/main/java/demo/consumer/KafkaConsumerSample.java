package demo.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Kafka Consumer 订阅主题示例代码
 *
 * @author yulinying
 * @since 2020/11/11
 */
@Slf4j
public class KafkaConsumerSample {
    
    
    public static void main(String[] args) {
        // consumer 配置
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("group.id", "my-consumer");
        properties.put("auto.commit.offset", "true");
        // 创建 consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        // 订阅主题
        consumer.subscribe(Collections.singletonList("test-topic"));
        // 轮询查询
        try{
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
                log.info("listen record count:{}", records.count());
                // 消费记录详细情况
                for (ConsumerRecord<String, String> record : records) {
                    log.info("consumer record: topic={}, value={}, offset={}, partition={}", record.topic(), record.value(), record.offset(), record.partition());
                }
            }
        } finally{
            consumer.commitSync();
            consumer.close();
        }
    }
    
}
