package cn.itcast.connector;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * Author itcast
 * Desc 演示Flink-Connector-Kafka
 * 从Kafka的topic1中消费数据,然后使用flink进行实时ETL过滤掉失败日志,最后将ETL之后的数据写到Kafka的topic2主题中
 */
public class ConnectorDemoKafka {
    public static void main(String[] args) throws Exception {
        //TODO 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //TODO 2.source
        //从Kafka的topic1消费数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","node1:9092");
        properties.setProperty("group.id", "test");
        properties.setProperty("auto.offset.reset","latest");//如果有offset记录从offset记录位置开始消费,如果没有则从latest最新的offset开始消费
        properties.setProperty("flink.partition-discovery.interval-millis","5000");//会开启一个后台线程每隔5s检测一下Kafka的分区情况(支持Kafka分区数变化)
        properties.setProperty("enable.auto.commit", "true");//自动提交offset
        properties.setProperty("auto.commit.interval.ms", "2000");//自动提交offset间隔
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>("topic1", new SimpleStringSchema(), properties);
        DataStreamSource<String> ds = env.addSource(kafkaSource);
        //TODO 3.transformation
        SingleOutputStreamOperator<String> etlDS = ds.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return value.contains("success");
            }
        });

        //TODO 4.sink
        etlDS.print();
        FlinkKafkaProducer<String> kafkaSink = new FlinkKafkaProducer<>("topic2", new SimpleStringSchema(), properties);
        etlDS.addSink(kafkaSink);

        //TODO 5.execution
        env.execute();
    }
}
/*
测试:
1.启动zk和kafka
2.准备主题
3.发送日志到kafka的topic1主题
/export/server/kafka/bin/kafka-console-producer.sh --broker-list node1:9092 --topic topic1
log:2021-04-24 success xxx
log:2021-04-24 success xxx
log:2021-04-24 success xxx
log:2021-04-24 fail xxx
4.观察kafka
/export/server/kafka/bin/kafka-console-consumer.sh --bootstrap-server node1:9092 --topic topic2
5.启动项目
 */
