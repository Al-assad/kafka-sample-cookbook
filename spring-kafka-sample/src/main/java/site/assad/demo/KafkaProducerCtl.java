package site.assad.demo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/**
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
    @GetMapping("/send/foo/{msg}")
    public String sendMsg(@PathVariable("msg") String msg){
        // 发送 kafka 信息
        Foo foo = new Foo(msg);
        kafkaTemplate.send("topic1", foo);
        return foo.toString();
    }
    
    
    
}
