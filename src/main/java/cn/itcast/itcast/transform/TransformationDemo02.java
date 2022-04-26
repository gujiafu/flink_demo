package cn.itcast.itcast.transform;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * Author itcast
 * Desc 演示Flink-DataStream-API-Transformation
 * union和connect
 */
public class TransformationDemo02 {
    public static void main(String[] args) throws Exception {
        //TODO 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //TODO 2.source
        DataStream<String> ds1 = env.fromElements("hadoop", "spark", "flink");
        DataStream<String> ds2 = env.fromElements("hadoop", "spark", "flink");
        DataStream<Long> ds3 = env.fromElements(1L, 2L, 3L);

        //TODO 3.transformation
        DataStream<String> result1 = ds1.union(ds2);
        //ds1.union(ds3);//注意:union只能合并同类型的数据

        ConnectedStreams<String, String> result2 = ds1.connect(ds2);
        ConnectedStreams<String, Long> result3 = ds1.connect(ds3);//注意:connect可以合并同类型或不同类型的数据


        //TODO 4.sink
        result1.print();
        //result2.print();//注意: connect返回的结果不能直接打印,需要再处理

        //result2.process(new CoProcessFunction<String, String, Object>())
        SingleOutputStreamOperator<String> finalResult = result3.map(new CoMapFunction<String, Long, String>() {
            @Override
            public String map1(String value) throws Exception {
                return "String:" + value;
            }

            @Override
            public String map2(Long value) throws Exception {
                return "Long:" + value;
            }
        });
        finalResult.print();
        //后面的Flink的项目中会有,这里先知道union和connect的区别即可


        //TODO 5.execution
        env.execute();
    }
}
