package cn.itcast.source;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * Author itcast
 * Desc 演示Flink-DataStream-API-基于集合的Source
 */
public class SourceDemo01 {
    public static void main(String[] args) throws Exception {
        //TODO 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //TODO 2.source
        //1.env.fromElements(可变参数);
        DataStream<String> ds1 = env.fromElements("hadoop", "spark", "flink");
        //2.env.fromColletion(各种集合);
        DataStream<String> ds2 = env.fromCollection(Arrays.asList("hadoop", "spark", "flink"));
        //3.env.generateSequence(始,结束);
        DataStream<Long> ds3 = env.generateSequence(1, 10);
        //4.env.fromSequence(开始,结束);
        DataStream<Long> ds4 = env.fromSequence(1, 10);
        //TODO 3.transformation
        //TODO 4.sink
        ds1.print();
        ds2.print();
//        ds3.print();
//        ds4.print();
        //TODO 5.execution
        env.execute();
    }
}
