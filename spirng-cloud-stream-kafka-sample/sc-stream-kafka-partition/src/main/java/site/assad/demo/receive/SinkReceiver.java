package site.assad.demo.receive;

import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import site.assad.demo.message.MyMessage;
import site.assad.demo.message.SampleTopic;

/**
 *
 * @author yulinying
 * @since 2020/11/11
 */
@Slf4j
@Component
public class SinkReceiver {
    
    @StreamListener(SampleTopic.INPUT)
    public void receive(MyMessage msg) {
        log.info("Received: " + msg.toString());
    }
    
    
}
