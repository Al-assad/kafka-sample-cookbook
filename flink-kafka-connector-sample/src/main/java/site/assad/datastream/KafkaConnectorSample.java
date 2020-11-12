package site.assad.datastream;

import com.github.houbb.word.checker.util.EnWordCheckers;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.calcite.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * 统计每 30s 中上游的输入语句的词频率，并输出到下游
 * kafka value 使用 string 格式，在 operator 中自定义进行 bean-json 序列化。反序列化
 *
 * @author yulinying
 * @since 2020/11/12
 */
@SuppressWarnings("DuplicatedCode")
public class KafkaConnectorSample {
    
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    public static void main(String[] args) throws Exception {
        // DataStream 获取，配置
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        
        // Kafka Consumer 配置构建
        Properties consumerProp = new Properties();
        consumerProp.setProperty("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        consumerProp.setProperty("group.id", "test");
        consumerProp.setProperty("enable.auto.commit", "true");
        FlinkKafkaConsumer<String> kConsumer = new FlinkKafkaConsumer<>("sentence-topic", new SimpleStringSchema(), consumerProp);
        kConsumer.setStartFromEarliest();
        
        // Kafka Datasource 读取语句、分词、校验，统计
        DataStream<Tuple2<String, Integer>> stream = env
                .addSource(kConsumer)
                .flatMap(new RichFlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        for (String word : value.split("\\s")) {
                            out.collect(word);
                        }
                    }
                })
                .filter(new RichFilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        return EnWordCheckers.isCorrect(value);
                    }
                })
                .map(new RichMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        return Tuple2.of(value, 1);
                    }
                })
                .keyBy(value -> value.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(30)))
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                });
        // 控制台输出，调试
        stream.print().setParallelism(1);
        
        // Kafka Producer 配置构建
        Properties producerProp = new Properties();
        producerProp.setProperty("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        producerProp.setProperty("acks", "all");
        FlinkKafkaProducer<String> kProducer = new FlinkKafkaProducer<>("word-count-topic", new SimpleStringSchema(), producerProp);
        kProducer.setWriteTimestampToKafka(true);
        
        // sink to kafka
        stream
                .map(new RichMapFunction<Tuple2<String, Integer>, String>() {
                    @Override
                    public String map(Tuple2<String, Integer> value) throws Exception {
                        return objectMapper.writeValueAsString(value);
                    }
                })
                .addSink(kProducer);
        
        env.execute("kafka source sample job");
        
    }
    
    
}
