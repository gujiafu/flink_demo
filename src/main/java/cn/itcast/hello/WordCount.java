package cn.itcast.hello;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Author itcast
 * Desc 演示Flink-DataSet-API完成批处理WordCount
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        //TODO 1.env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //TODO 2.source
        DataSet<String> linesDS = env.fromElements(
                "itcast hadoop spark",
                "itcast hadoop spark",
                "itcast hadoop",
                "itcast"
        );
        //TODO 3.transformation
        //每一行按照空格切分并压扁
        /*
        public interface FlatMapFunction<T, O> extends Function, Serializable {
           void flatMap(T value, Collector<O> out) throws Exception;
       }
         */
        DataSet<String> wordsDS = linesDS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                //value就是进来的每一行
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });
        //每个单词记为1
        /*
        public interface MapFunction<T, O> extends Function, Serializable {
            O map(T value) throws Exception;
        }
         */
        DataSet<Tuple2<String, Integer>> wordAndOneDS = wordsDS.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        });
        //分组
        UnsortedGrouping<Tuple2<String, Integer>> groupedDS = wordAndOneDS.groupBy(0);
        /*
        public interface KeySelector<IN, KEY> extends Function, Serializable {
            KEY getKey(IN value) throws Exception;
        }
         */
       /* UnsortedGrouping<Tuple2<String, Integer>> groupedDS = wordAndOneDS.groupBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });*/

        //聚合
        AggregateOperator<Tuple2<String, Integer>> result = groupedDS.sum(1);

        //TODO 4.sink
        result.print();

        //TODO 5.execute
        //latest call to 'execute()', 'count()', 'collect()', or 'print()'.
        //env.execute();//注意:在DataSet模式下,调用了print就不用再调用execute
    }
}
