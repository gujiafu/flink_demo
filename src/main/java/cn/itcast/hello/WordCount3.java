package cn.itcast.hello;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * Author itcast
 * Desc 演示Flink-DataStream-API完成批处理WordCount--Java8-Lambda
 * 注意:在flink1.12的时候DataStream既支持批处理也支持流处理
 */
public class WordCount3 {
    public static void main(String[] args) throws Exception {
        //TODO 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //不设置模式是流模式
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);//自动
        //env.setRuntimeMode(RuntimeExecutionMode.STREAMING);//流
        //env.setRuntimeMode(RuntimeExecutionMode.BATCH);//批
        //如果不知道该用什么模式就用AUTOMATIC自动
        //TODO 2.source
        DataStream<String> linesDS = env.fromElements(
                "itcast hadoop spark",
                "itcast hadoop spark",
                "itcast hadoop",
                "itcast");
        //TODO 3.transformation
        //每一行按照空格切分并压扁
        /*
        public interface FlatMapFunction<T, O> extends Function, Serializable {
            void flatMap(T value, Collector<O> out) throws Exception;
        }
         */
        /*DataStream<String> wordsDS = linesDS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });*/
        //注意:Java8中的函数式编程Lambda表达式的语法: (参数)->{函数体}
        SingleOutputStreamOperator<String> wordsDS = linesDS.flatMap((String value, Collector<String> out) -> Arrays.stream(value.split(" ")).forEach(out::collect)).returns(Types.STRING);
        //上面的写法了解

        //每个单词记为1
        /*
        public interface MapFunction<T, O> extends Function, Serializable {
            O map(T value) throws Exception;
        }
         */
        /*DataStream<Tuple2<String, Integer>> wordAndOneDS = wordsDS.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        });*/
        DataStream<Tuple2<String, Integer>> wordAndOneDS = wordsDS.map((String value)->Tuple2.of(value, 1)).returns(Types.TUPLE(Types.STRING, Types.INT));
        //上面的写法了解


        //分组
        //注意:在DataSet中分组用groupBy,在DataStream中分组用keyBy
        //KeyedStream<Tuple2<String, Integer>, Tuple> groupedDS = wordAndOneDS.keyBy(0);
        /*KeyedStream<Tuple2<String, Integer>, String> groupedDS = wordAndOneDS.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });*/
        /*
        public interface KeySelector<IN, KEY> extends Function, Serializable {
            KEY getKey(IN value) throws Exception;
        }
         */
        KeyedStream<Tuple2<String, Integer>, String> groupedDS = wordAndOneDS.keyBy((Tuple2<String, Integer> value)->value.f0);

        //聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = groupedDS.sum(1);
        //TODO 4.sink
        result.print();
        //TODO 5.execute
        //注意:在DataStream中execute不能省略
        env.execute();
    }
}
