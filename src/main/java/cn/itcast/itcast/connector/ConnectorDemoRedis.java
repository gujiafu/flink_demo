package cn.itcast.itcast.connector;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

/**
 * Author itcast
 * Desc 演示Flink-Connector-Redis
 */
public class ConnectorDemoRedis {
    public static void main(String[] args) throws Exception {
        //TODO 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //TODO 2.source
        DataStream<String> ds = env.socketTextStream("node1", 9999);

        //TODO 3.transformation
        DataStream<Tuple2<String, Integer>> resultDS = ds.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        }).keyBy(0).sum(1);

        //TODO 4.sink
        resultDS.print();

        FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build();
        resultDS.addSink(new RedisSink<Tuple2<String, Integer>>(jedisPoolConfig,new MyRedisMapper()));

        //TODO 5.execution
        env.execute();
    }

    //自定义Redis命令和数据的映射关系
    public static class MyRedisMapper implements RedisMapper<Tuple2<String, Integer>>{
        //指定命令
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET,"wc");
        }

        @Override
        public String getKeyFromData(Tuple2<String, Integer> t) {
            return t.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, Integer> t) {
            return t.f1.toString();
        }
    }
}
