package demo.producer;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka Producer 自定义 Partition 示例
 *
 * @author yulinying
 * @since 2020/11/10
 */
@Slf4j
public class KafkaProducerSampleCustomPartition {
    
    static KafkaProducer<String, String> kafkaProducer;
    
    /**
     * 初始化 KafkaProducer
     */
    @BeforeAll
    public static void initKafkaProducer() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "broker1:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 设置 Partition 策略实现
        properties.put("Partitioner.class", "demo.producer.MyPartition");
        kafkaProducer = new KafkaProducer<>(properties);
    }
    
    /**
     * 关闭 KafkaProducer
     */
    @AfterAll
    public static void destroyKafkaProducer() {
        kafkaProducer.flush();
        kafkaProducer.close();
    }
    
    /**
     * 简单消息发送
     */
    @Test
    public void sendSimpleMsg() {
        ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", "greeting", "hello world");
        kafkaProducer.send(record);
    }
    
    
    
}
