package site.assad.demo.receive;

import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.support.ErrorMessage;
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
    
    /**
     * 监听主题，强制抛出异常
     */
    @StreamListener(SampleTopic.INPUT)
    public void receive(MyMessage msg) {
        log.info("Received: " + msg.toString());
        throw new RuntimeException("this is receive error！");
    }
    
    /**
     * 监听死信队列
     */
    @ServiceActivator(inputChannel = "test-topic1.group-default.errors")
    public void handleError(ErrorMessage errorMessage) {
        log.error("[handleError][payload：{}]", errorMessage.getPayload().getMessage());
        log.error("[handleError][originalMessage：{}]", errorMessage.getOriginalMessage());
        log.error("[handleError][headers：{}]", errorMessage.getHeaders());
    }
    
    
    
}
