package site.assad.demo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

/**
 * Kafka Producer 发送消息
 *
 * @author yulinying
 * @since 2020/11/11
 */
@RestController
public class KafkaProducerCtl {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;
    
    /**
     * 发送消息
     */
    @PostMapping("/send/foo/{msg}")
    public String sendMsg(@PathVariable("msg") String msg){
        // 发送 kafka 信息
        Foo foo = new Foo(msg);
        kafkaTemplate.send("topic1", foo);
        return "Send msg: " + foo.toString();
    }
    
    /**
     * 发送消息
     */
    @PostMapping("/send/foo2/{msg}")
    public String sendMsg2(@PathVariable("msg") String msg){
        // 发送 kafka 信息
        Foo foo = new Foo(msg);
        String key = UUID.randomUUID().toString();
        kafkaTemplate.send("topic2", key, foo);
        return "Send msg: " + key + ", " + foo.toString();
    }
    
    
}
