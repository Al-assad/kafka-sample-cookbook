package demo.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Kafka Consumer 订阅主题，手动提交 offset 示例
 *
 * @author yulinying
 * @since 2020/11/11
 */
@Slf4j
public class KafkaConsumerOffsetCommitSample {
    
    
    /**
     * 获取 KafkaConsumer
     */
    public static KafkaConsumer<String, String> getKafkaConsumer() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("group.id", "my-consumer");
        // 关闭自动提交
        properties.put("auto.commit.offset", "false");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        return consumer;
    }
    
    
    /**
     * 手动提交 offset 示例，同步提交
     */
    @Test
    public void commitSyncTest() {
        KafkaConsumer<String, String> consumer = getKafkaConsumer();
        consumer.subscribe(Collections.singletonList("test-topic"));
        // 轮询查询
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
                log.info("listen record count:{}", records.count());
                records.forEach(record -> log.info("consumer record: topic={}, value={}, offset={}, partition={}",
                        record.topic(), record.value(), record.offset(), record.partition()));
                // 手动同步提交 offset
                consumer.commitSync();
                
                // 手动同步提交，指定超时时间
                // consumer.commitSync(Duration.ofSeconds(10));
            }
        } finally {
            consumer.commitSync();
            consumer.close();
        }
    }
    
    
    /**
     * 手动提交 offset 示例，异步提交
     */
    @Test
    public void commitAsyncTest() {
        KafkaConsumer<String, String> consumer = getKafkaConsumer();
        consumer.subscribe(Collections.singletonList("test-topic"));
        // 轮询查询
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
                log.info("listen record count:{}", records.count());
                records.forEach(record -> log.info("consumer record: topic={}, value={}, offset={}, partition={}",
                        record.topic(), record.value(), record.offset(), record.partition()));
                // 手动异步提交 offset
                consumer.commitAsync();
            }
        } finally {
            consumer.commitSync();
            consumer.close();
        }
    }
    
    
}
