package cep;

import cn.itcast.cep.bean.OrderEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * 需求：
 * 用户下单以后，应该设置订单失效时间，用来提高用户的支付意愿如果用户下单15分钟未支付，则输出监控信息
 */
public class OrderTimeoutTest {

    public static void main(String[] args) throws Exception {
        /**
         * 实现步骤：
         * 1.获取流处理执行环境
         * 2.设置并行度,设置事件时间
         * 3.加载数据源,提取事件时间
         * 4.定义匹配模式followedBy，设置时间长度
         * 5.匹配模式（分组）
         * 6.设置侧输出流
         * 7.数据处理(获取begin数据)
         * 8.打印（正常/超时订单）
         * 9.触发执行
         */

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        KeyedStream<OrderEvent, Integer> source = env.fromElements(
                new OrderEvent(1, "create", 1558430842000L),//2019-05-21 17:27:22
                new OrderEvent(2, "create", 1558430843000L),//2019-05-21 17:27:23
                new OrderEvent(2, "other", 1558430845000L), //2019-05-21 17:27:25
                new OrderEvent(2, "pay", 1558430850000L),   //2019-05-21 17:27:30
                new OrderEvent(1, "pay", 1558431920000L),   //2019-05-21 17:45:20)
                new OrderEvent(3, "create", 1558431920000L))    //2019-05-21 17:45:20)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<OrderEvent>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(OrderEvent element) {
                        return element.getEventTime();
                    }
                }).keyBy(OrderEvent::getOrderId);

        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("begin").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return value.getStatus().equals("create");
            }
        }).followedBy("end").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return value.getStatus().equals("pay");
            }
        }).within(Time.minutes(15));

        PatternStream<OrderEvent> cep = CEP.pattern(source, pattern);

        OutputTag<OrderEvent> outputTag = new OutputTag<OrderEvent>("outTime", TypeInformation.of(OrderEvent.class));

        SingleOutputStreamOperator<OrderEvent> result = cep.select(outputTag, new PatternTimeoutFunction<OrderEvent, OrderEvent>() {
            @Override
            public OrderEvent timeout(Map<String, List<OrderEvent>> map, long l) throws Exception {
                return map.get("begin").iterator().next();
            }
        }, new PatternSelectFunction<OrderEvent, OrderEvent>() {
            @Override
            public OrderEvent select(Map<String, List<OrderEvent>> map) throws Exception {
                return map.get("begin").iterator().next();
            }
        });

        result.print("正常： ");
        result.getSideOutput(outputTag).print("超时： ");

        env.execute();
    }
}
