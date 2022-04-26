package cn.itcast.itcast.transform;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Author itcast
 * Desc 演示Flink-DataStream-API-Transformation
 * 使用map/flatMap/filter/sum/reduce...完成如下功能
 * 对流中的数据进行WordCount,并排除敏感词TMD
 */
public class TransformationDemo01 {
    public static void main(String[] args) throws Exception {
        //TODO 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //TODO 2.source
        DataStreamSource<String> ds = env.socketTextStream("node1", 9999);
        //TODO 3.transformation
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDS = ds.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        }).filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return !value.equals("TMD");//排除敏感词TMD
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        })//.keyBy(0)
         .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
             @Override
             public String getKey(Tuple2<String, Integer> value) throws Exception {
                 return value.f0;
             }
         })//.sum(1)
         .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
             @Override
             public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                 return Tuple2.of(value1.f0, value1.f1 + value2.f1);
             }
         });
        //TODO 4.sink
        resultDS.print();
        //TODO 5.execution
        env.execute();
    }
}
