package cn.itcast.transform;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Author itcast
 * Desc 演示Flink-DataStream-API-Transformation
 * Flink中的rebalance有点类似于Spark中的reparation,但是更便捷一些, 不一样的地方是:
 * rebalance可以将数据尽量重新分布均匀,在一定程度上解决数据倾斜的问题
 * 也就是说在Flink中提供了解决数据倾斜的简单API--rebalance
 */
public class TransformationDemo04 {
    public static void main(String[] args) throws Exception {
        //TODO 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(4);
        int parallelism = env.getParallelism();//获取默认并行度
        System.out.println("并行度为:"+parallelism);


        //TODO 2.source
        DataStream<Long> longDS = env.fromSequence(0, 100);

        //TODO 3.transformation
        //下面的filter操作相当于将数据分到不同的线程/机器上执行过滤操作,也就是随机分配了一下数据,有可能出现数据倾斜
        DataStream<Long> filterDS = longDS.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long num) throws Exception {
                return num > 10;
            }
        });
        //查看一下数据被分配到哪个分区中了(获取出子任务id/分区编号),并统计一下每个分区有多少条数据!--可能会有数据倾斜
        SingleOutputStreamOperator<Tuple2<Integer, Integer>> result1 =
                filterDS.map(new RichMapFunction<Long, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(Long value) throws Exception {
                int subtaskId = getRuntimeContext().getIndexOfThisSubtask();
                return Tuple2.of(subtaskId, 1);
            }
        }).keyBy(t -> t.f0).sum(1);//按照分区编号分组并统计每个分区编号中有多少数据!

        //执行rebalance重分区之后再统计每个分区有多少条数据!---使用rebalance之后在一定程度上解决了数据倾斜
        SingleOutputStreamOperator<Tuple2<Integer, Integer>> result2 =
                filterDS.rebalance()
                        .map(new RichMapFunction<Long, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(Long value) throws Exception {
                int subtaskId = getRuntimeContext().getIndexOfThisSubtask();
                return Tuple2.of(subtaskId, 1);
            }
        }).keyBy(t -> t.f0).sum(1);//按照分区编号分组并统计每个分区编号中有多少数据!

        //TODO 4.sink
        //result1.print();
        result2.print();
        //TODO 5.execution
        env.execute();
    }
}
