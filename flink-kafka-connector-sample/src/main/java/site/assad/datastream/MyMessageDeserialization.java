package site.assad.datastream;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.calcite.shaded.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * MyMessage 自定义对象解析器
 *
 * @author yulinying
 * @since 2020/11/12
 */
public class MyMessageDeserialization implements DeserializationSchema<MyMessage> {
    
    private static final long serialVersionUID = 4364310377863695997L;
    private final ObjectMapper objectMapper = new ObjectMapper();
    @Override
    public MyMessage deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, MyMessage.class);
    }
    
    @Override
    public boolean isEndOfStream(MyMessage nextElement) {
        return false;
    }
    
    @Override
    public TypeInformation<MyMessage> getProducedType() {
        return TypeInformation.of(MyMessage.class);
    }
}
