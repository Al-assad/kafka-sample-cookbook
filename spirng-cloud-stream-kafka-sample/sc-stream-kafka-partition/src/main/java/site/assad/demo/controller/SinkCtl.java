package site.assad.demo.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import site.assad.demo.message.MyMessage;
import site.assad.demo.message.SampleTopic;

/**
 *
 * @author yulinying
 * @since 2020/11/11
 */
@RestController
public class SinkCtl {
    
    @Autowired
    SampleTopic sampleTopic;
    
    @PostMapping("/send/{msg}")
    public String sendMsg(@PathVariable("msg") String msg){
        MyMessage messgae = new MyMessage(msg);
        // 自定义计算 partition
        int partition = msg.hashCode() % 3;
        sampleTopic.output().send(MessageBuilder.withPayload(messgae)
                .setHeader("partition", partition)
                .build());
        return "Send msg: " + messgae;
    }
    
}
