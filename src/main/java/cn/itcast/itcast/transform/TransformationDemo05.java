package cn.itcast.itcast.transform;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * Author itcast
 * Desc 演示Flink-DataStream-API-Transformation-其他分区
 */
public class TransformationDemo05 {
    public static void main(String[] args) throws Exception {
        //TODO 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        System.out.println("env.getParallelism():"+env.getParallelism());

        //TODO 2.source
        DataStream<String> linesDS = env.readTextFile("data/input/words.txt");
        //TODO 3.transformation
        SingleOutputStreamOperator<Tuple2<String, Integer>> tupleDS = linesDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });

        //演示各种分区
        DataStream<Tuple2<String, Integer>> result1 = tupleDS.global();
        DataStream<Tuple2<String, Integer>> result2 = tupleDS.broadcast();
        DataStream<Tuple2<String, Integer>> result3 = tupleDS.forward();
        DataStream<Tuple2<String, Integer>> result4 = tupleDS.shuffle();
        DataStream<Tuple2<String, Integer>> result5 = tupleDS.rebalance();
        DataStream<Tuple2<String, Integer>> result6 = tupleDS.rescale();
        //自定义分区
        DataStream<Tuple2<String, Integer>> result7 = tupleDS.partitionCustom(new MyPartitioner(),t->t.f0);

        //TODO 4.sink
        /*result1.print();
        result2.print();
        result3.print();
        result4.print();
        result5.print();
        result6.print();*/
        result7.print();
        //TODO 5.execution
        env.execute();
    }

    public static class MyPartitioner implements Partitioner<String> {
        //自定义如何分区
        @Override
        public int partition(String key, int numPartitions) {
            //如知行教育中不同的地区,数据量不一样,可以将一线城市用多一点分区,其他城市全部放在一个分区
            //if(城市属于一线城市){return random.nextInt(5)} else{return 5}
            //也可以自己定义其他规则:
            //如随机分
            System.out.println("numPartitions:"+numPartitions);
            return new Random().nextInt(numPartitions);
            //如全部放到1个分区
            //return 0;
        }
    }
}
