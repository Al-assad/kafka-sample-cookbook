package demo.producer;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Kafka 消息发送示例
 *
 * @author yulinying
 * @since 2020/11/10
 */
@Slf4j
public class KafkaProducerSample {
    
    static KafkaProducer<String, String> kafkaProducer;
    
    /**
     * 初始化 KafkaProducer
     */
    @BeforeAll
    public static void initKafkaProducer() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        // properties.put("bootstrap.servers","broker1:9092,broker2:9092");
        properties.put("acks", "all");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
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
    
    /**
     * 发送消息，使用应用程序自定义时间戳、自定义分区
     */
    @Test
    public void sendMsgWithPartitionAndTimestamp() {
        ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", 1, new Date().getTime(),
                "greeting", "hello world, my friend");
        kafkaProducer.send(record);
    }
    
    /**
     * 同步发送消息，获取发送消息结果、元信息
     */
    @Test
    public void sendMsgSync() throws ExecutionException, InterruptedException {
        ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", "this is sync message!");
        RecordMetadata recordMetadata = kafkaProducer.send(record).get();
        log.info("record meta: {}", recordMetadata.toString());
    }
    
    /**
     * 异步发送消息
     */
    @Test
    public void sendMsgAsync(){
        ProducerRecord<String, String> record = new ProducerRecord<>("test-topic",  "this is async message!");
        kafkaProducer.send(record, new MyCallback());
    }
    
    public class MyCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception != null) {
                log.error(exception.getMessage());
            }
            log.info("callback record meta: {}", metadata.toString());
        }
    }
    
    
    
    
}
