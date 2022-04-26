package cn.itcast.itcast.state;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Author itcast
 * Desc 演示Flink-State-ManagedState-KeyedState
 * 使用自定义状态完成求数据的最大值
 */
public class StateDemo01 {
    public static void main(String[] args) throws Exception {
        //TODO 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //TODO 2.source
        DataStreamSource<Tuple2<String, Long>> tupleDS = env.fromElements(
                Tuple2.of("北京", 1L),
                Tuple2.of("上海", 2L),
                Tuple2.of("北京", 6L),
                Tuple2.of("上海", 8L),
                Tuple2.of("北京", 3L),
                Tuple2.of("北京", 9L),
                Tuple2.of("上海", 5L),
                Tuple2.of("上海", 1L),
                Tuple2.of("上海", 2L),
                Tuple2.of("上海", 4L)
        );

        //TODO 3.transformation
        //求每个城市的最大值
        //方法1:使用Flink自带的API(状态底层已经自动管理了)--开发中直接使用这个
        //注意: max和maxBy都能求最大值,区别在于:maxBy能保证key和value是对的,max只能保证value是对的,不能保证key是对的,如果要求最大值和最大值对应的key,得用maxBy
        SingleOutputStreamOperator<Tuple2<String, Long>> result1 = tupleDS.keyBy(t -> t.f0).max(1);
        SingleOutputStreamOperator<Tuple2<String, Long>> result2 = tupleDS.keyBy(t -> t.f0).maxBy(1);

        //方法2:使用自定义的状态--学习时了解一下原理
        SingleOutputStreamOperator<Tuple3<String, Long, Long>> result3 = tupleDS.keyBy(t -> t.f0).map(new RichMapFunction<Tuple2<String, Long>, Tuple3<String, Long, Long>>() {
            //-1.定义一个状态用来存放最大值
            private ValueState<Long> maxValueState = null;

            //-2.初始化状态
            @Override
            public void open(Configuration parameters) throws Exception {
                //创建状态描述器
                ValueStateDescriptor<Long> stateDescriptor = new ValueStateDescriptor<>("maxValueState", Long.class);
                //根据状态描述器初始化状态
                maxValueState = getRuntimeContext().getState(stateDescriptor);
            }

            //-3.使用状态
            @Override
            public Tuple3<String, Long, Long> map(Tuple2<String, Long> value) throws Exception {
                Long currentValue = value.f1;//当前进来的值
                Long maxValue = maxValueState.value();//记录中的值
                if (maxValue == null || currentValue > maxValue) {
                    maxValue = currentValue;
                }
                //-4.更新状态
                maxValueState.update(maxValue);

                return Tuple3.of(value.f0, value.f1, maxValue);
            }
        });

        //TODO 4.sink
        result1.print("result1");
        result2.print("result2");
        result3.print("result3");

        //TODO 5.execution
        env.execute();
    }
}
