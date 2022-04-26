package cn.itcast.sql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Author itcast
 * Desc
 * 从Kafka的input_kafka主题中消费数据
 * 并过滤出状态为success的数据
 * 再写入到Kafka的output_kafka主题中
 */
public class FlinkSQLDemo04 {
    public static void main(String[] args) throws Exception {
        //TODO 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        //TODO 2.source/准备表View/Table
        //从Kafka的input_kafka主题读取数据到inputTable中
        TableResult inputTable = tenv.executeSql(
                "CREATE TABLE input_kafka (\n" +
                        "  `user_id` BIGINT,\n" +
                        "  `page_id` BIGINT,\n" +
                        "  `status` STRING\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'input_kafka',\n" +
                        "  'properties.bootstrap.servers' = 'node1:9092',\n" +
                        "  'properties.group.id' = 'testGroup',\n" +
                        "  'scan.startup.mode' = 'latest-offset',\n" +
                        "  'format' = 'json'\n" +
                        ")"
        );
        //将outputTable中的数据写入到Kafka的output_kafka主题
        TableResult outputTable = tenv.executeSql(
                "CREATE TABLE output_kafka (\n" +
                        "  `user_id` BIGINT,\n" +
                        "  `page_id` BIGINT,\n" +
                        "  `status` STRING\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'output_kafka',\n" +
                        "  'properties.bootstrap.servers' = 'node1:9092',\n" +
                        "  'format' = 'json',\n" +
                        "  'sink.partitioner' = 'round-robin'\n" +
                        ")"
        );

        //TODO 3.transformation/查询SQL风格和Table风格(SQL/DSL)
        //把status为success的数据过滤出来
        Table resultTable = tenv.sqlQuery("select user_id,page_id,status from input_kafka where status='success'");

        //TODO 4.sink
        DataStream<Tuple2<Boolean, Row>> result = tenv.toRetractStream(resultTable, Row.class);
        //打印到控制台
        result.print();

        //将数据写入到Kafka
        tenv.executeSql("insert into output_kafka select * from "+resultTable);


        //TODO 5.execute
        env.execute();
    }

}
