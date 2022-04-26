package cn.itcast.itcast.extend;

import org.apache.commons.lang3.SystemUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.util.Collector;

import java.util.Properties;
import java.util.Random;

/**
 * Author itcast
 * Desc 演示
 * Source-->Transformation-->Sink的Flink-End-To-End Exactly Once
 * 以 Kafka + Flink + Kafka 为例
 */
public class KafkFlinkKafka {
    public static void main(String[] args) throws Exception {
        //TODO 1.env(开启Checkpoint)
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //System.out.println(env.getParallelism());
        //===========Checkpoint参数设置====
        //===========类型1:必须参数=============
        //设置Checkpoint的时间间隔为1000ms做一次Checkpoint/其实就是每隔1000ms发一次Barrier!
        env.enableCheckpointing(1000);
        //设置State状态存储介质
        if (SystemUtils.IS_OS_WINDOWS) {
            env.setStateBackend(new FsStateBackend("file:///D:/ckp"));
        } else {
            env.setStateBackend(new FsStateBackend("hdfs://node1:8020/flink-checkpoint/checkpoint"));
        }
        //===========类型2:建议参数===========
        //设置两个Checkpoint 之间最少等待时间,如设置Checkpoint之间最少是要等 500ms(为了避免每隔1000ms做一次Checkpoint的时候,前一次太慢和后一次重叠到一起去了)
        //如:高速公路上,每隔1s关口放行一辆车,但是规定了两车之前的最小车距为500m
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);//默认是0
        //设置如果在做Checkpoint过程中出现错误，是否让整体任务失败：true是  false不是
        //env.getCheckpointConfig().setFailOnCheckpointingErrors(false);//默认是true
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(10);//默认值为0，表示不容忍任何检查点失败
        //设置是否清理检查点,表示 Cancel 时是否需要保留当前的 Checkpoint，默认 Checkpoint会在作业被Cancel时被删除
        //ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION：true,当作业被取消时，删除外部的checkpoint(默认值)
        //ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION：false,当作业被取消时，保留外部的checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //===========类型3:直接使用默认的即可===============
        //设置checkpoint的执行模式为EXACTLY_ONCE(默认)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //设置checkpoint的超时时间,如果 Checkpoint在 60s内尚未完成说明该次Checkpoint失败,则丢弃。
        env.getCheckpointConfig().setCheckpointTimeout(60000);//默认10分钟
        //设置同一时间有多少个checkpoint可以同时执行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);//默认为1

        //=============重启策略===========
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));

        //TODO 2.source(使用FlinkKafkaConsumer底层使用Checkpoint维护offset)
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.52.100:9092");
        properties.setProperty("group.id", "test");
       /* properties.setProperty("auto.offset.reset", "latest");//如果有offset记录从offset记录位置开始消费,如果没有则从latest最新的offset开始消费
        properties.setProperty("flink.partition-discovery.interval-millis", "5000");//会开启一个后台线程每隔5s检测一下Kafka的分区情况(支持Kafka分区数变化)
        properties.setProperty("enable.auto.commit", "true");//自动提交offset
        properties.setProperty("auto.commit.interval.ms", "2000");//自动提交offset间隔*/
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>("topic1", new SimpleStringSchema(), properties);

        DataStream<String> kafkaDS = env.addSource(kafkaSource);
        kafkaDS.print("kafkaDS");

        //TODO 3.transformation(Flink-有状态API默认就是支持State/Checkpoint)
        DataStream<String> resultDS = kafkaDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    Random random = new Random();
                    int i = random.nextInt(5);
                    if (i > 3) {
                        System.out.println("出bug了");
                        throw new RuntimeException("出bug了");
                    }
                    out.collect(Tuple2.of(word, 1));
                }
            }
        }).keyBy(0).sum(1).map(new MapFunction<Tuple2<String, Integer>, String>() {
            @Override
            public String map(Tuple2<String, Integer> value) throws Exception {
                return value.f0 + "::" + value.f1;
            }
        });


        //TODO 4.sink(使用FlinkKafkaProducer底层使用两阶段事务提交+Checkpoint实现)
        resultDS.print();
        //这是以前的写法
        Properties properties2 = new Properties();
        properties2.setProperty("bootstrap.servers", "192.168.52.100:9092");
        properties2.setProperty("transaction.timeout.ms", "5000");//事务超时时间配置
        FlinkKafkaProducer kafkaSink = new FlinkKafkaProducer("topic2",
                new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()), //序列化约束现在使用String即可,后面项目中使用自定义约束
                properties2,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        //现在要执行容错语义

        resultDS.addSink(kafkaSink);

        //TODO 5.execute
        env.execute();

        //测试:
        //1.创建主题 /export/servers/kafka_2.11-1.0.0/bin/kafka-topics.sh --zookeeper node01:2181 --create --replication-factor 1 --partitions 1 --topic topic1
        //1.创建主题 /export/servers/kafka_2.11-1.0.0/bin/kafka-topics.sh --zookeeper node01:2181 --create --replication-factor 1 --partitions 1 --topic topic2
        //2.开启控制台生产者 /export/servers/kafka_2.11-1.0.0/bin/kafka-console-producer.sh --broker-list node01:9092 --topic topic1
        //3.开启控制台消费者 /export/servers/kafka_2.11-1.0.0/bin/kafka-console-consumer.sh --bootstrap-server node01:9092 --topic topic2

    }
}
