package site.assad.demo.message;

import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.SubscribableChannel;

/**
 * 消息主题
 *
 * @author yulinying
 * @since 2020/11/11
 */
public interface SampleTopic {
    
    String OUTPUT = "sample-topic-output";
    String INPUT = "sample-topic-input";
    
    /**
     * 发布消息通道
     */
    @Output(OUTPUT)
    MessageChannel output();
    
    /**
     * 订阅消息通道
     */
    @Input(INPUT)
    SubscribableChannel input();
    
    
}
