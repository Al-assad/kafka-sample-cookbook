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
    
    @StreamListener(value = SampleTopic.INPUT, condition = "headers['version']=='v1'")
    public void receive(@Payload MyMessage msg,@Header("version") String version) {
        log.info("Received: {}, version={}",msg.toString(), version);
    }
    
    @StreamListener(value = SampleTopic.INPUT, condition = "headers['version']=='v2'")
    public void receive3(@Payload MyMessage msg,@Header("version") String version) {
        log.info("Received: {}, version={}",msg.toString(), version);
    }

    
}
