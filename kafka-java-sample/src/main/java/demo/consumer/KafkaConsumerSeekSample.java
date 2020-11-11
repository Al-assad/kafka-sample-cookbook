package demo.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Kafka Consumer 订阅主题，指定 partition seek
 *
 * @author yulinying
 * @since 2020/11/11
 */
@Slf4j
public class KafkaConsumerSeekSample {
    
    
    /**
     * 获取 KafkaConsumer
     */
    public KafkaConsumer<String, String> getKafkaConsumer() {
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
     * 消费记录
     */
    public void consumerRecord(KafkaConsumer<String, String> consumer) {
        // 轮询查询
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
                records.forEach(record -> log.info("consumer record: topic={}, value={}, offset={}, partition={}", record.topic(), record.value(), record.offset(), record.partition()));
            }
        } finally {
            consumer.commitSync();
            consumer.close();
        }
    }
    
    /**
     * seek offset 示例：所有分区从起步初始 offset 开始消费
     */
    @Test
    public void seekBeginningTest(){
        KafkaConsumer<String, String> consumer = getKafkaConsumer();
        List<PartitionInfo> partitionInfos = consumer.partitionsFor("test-topic");
        consumer.subscribe(Collections.singletonList("test-topic"));
        // 获取 topic 所有分区
        List<TopicPartition> topicPartitions = partitionInfos.stream().map(info-> new TopicPartition(info.topic(), info.partition())).collect(Collectors.toList());
        // 所有分区 offset 设置到起始
        consumer.seekToBeginning(topicPartitions);
        // 消费主题
        consumerRecord(consumer);
    }
    
    /**
     * seek offset 示例，设置指定所有，指定 offset
     */
    @Test
    public void seekCustomTest(){
        KafkaConsumer<String, String> consumer = getKafkaConsumer();
        consumer.subscribe(Collections.singletonList("test-topic"));
        consumer.seek(new TopicPartition("test-topic", 1), 23);
        consumerRecord(consumer);
    }
    
    
    
}
