package site.assad.table;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


/**
 * Kafka Connector SQL 示例
 * 从 Kafka word-count-topic 主题获取实时词频信息，输出 topN 统计信息到 word-count-stat-topic 主题
 *
 * @author yulinying
 * @since 2020/11/12
 */
public class KafkaSqlSample {
    
    public static void main(String[] args) throws Exception {
        
        // Flink Table 环境获取，使用 Blink Planner
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        EnvironmentSettings envSetting = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, envSetting);
        
        // source table ddl
        String sourceDdl = "CREATE TABLE Message (\n" +
                "    f0 STRING,\n" +
                "    f1 INT, \n" +
                "    ts TIMESTAMP(3) \n" +
                "    WATERMARK FOR ts AS ts - INTERVAL '3' SECOND\\n" +
                ") WITH (\n" +
                "     'connector' = 'kafka',\n" +
                "    'topic' = 'word-count-topic',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092,localhost:9093,localhost:9094',\n" +
                "    'properties.group.id' = 'testGroup',\n" +
                "    'format' = 'json',\n" +
                "    'scan.startup.mode' = 'earliest-offset'\n" +
                ")";
        tEnv.executeSql(sourceDdl);
        
        
        // sink table ddl
        String sinkDdl =  "CREATE TABLE wordCountStat (\n" +
                "    word STRING,\n" +
                "    wordCount INT, \n" +
                "    startTs TIMESTAMP(3), \n" +
                "    endTs TIMESTAMP(3) \n" +
                ") WITH (\n" +
                "     'connector' = 'kafka',\n" +
                "    'topic' = 'word-count-stat-topic',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092,localhost:9093,localhost:9094',\n" +
                "    'properties.group.id' = 'testGroup',\n" +
                "    'format' = 'json',\n" +
                "    'scan.startup.mode' = 'earliest-offset'\n" +
                ")";
        
        
        // 查询 5s 内词频最高的记录
        String topNSql = "SELECT f0 AS word, f1 AS wordCount, MIN(ts) as startTs, MAX(ts) as endTs \n" +
                "FROM Message\n" +
                "GROUP BY TUMBLE(ts, INTERVAL '5' SECOND)\n" +
                "ORDER BY wordCount DESC\n" +
                "LIMIT 3";
        TableResult result = tEnv.sqlQuery(topNSql).execute();
        result.print();
        
        // 上述查询结果插入 sink
        String insertSql = "INSET INTO wordCountStat " + topNSql;
        TableResult result2 = tEnv.executeSql(insertSql);
        result2.print();
        
        env.execute("Kafka Connector SQL Job");
        
    }
}
