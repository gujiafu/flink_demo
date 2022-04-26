package cn.itcast.watermark;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;

/**
 * Author itcast
 * Desc
 * 实时模拟订单数据(订单id,用户id,订单金额,事件时间)
 * 每隔5s,计算最近5s内,每个用户的订单总金额
 * 并使用Watermark+EventTime来解决一定程度上的数据/延迟到达乱序问题
 */
public class WatermarkDemo01 {
    public static void main(String[] args) throws Exception {
        //TODO 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //TODO 2.source
        DataStream<Order> orderDS = env.addSource(new SourceFunction<Order>() {
            private boolean flag = true;

            @Override
            public void run(SourceContext<Order> ctx) throws Exception {
                Random ran = new Random();
                while (flag) {
                    String oid = UUID.randomUUID().toString();
                    int uid = ran.nextInt(3);
                    int money = ran.nextInt(100);
                    //随机模拟一些延迟
                    long eventTime = System.currentTimeMillis() - (1000 * ran.nextInt(3));
                    ctx.collect(new Order(oid,uid,money,eventTime));
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                this.flag = false;
            }
        });

        //TODO 3.transformation
        //https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/event_timestamps_watermarks.html
        //实时模拟订单数据(订单id,用户id,订单金额,事件时间)
        //每隔5s,计算最近5s内,每个用户的订单总金额
        //并使用Watermark+EventTime来解决一定程度上的数据/延迟到达乱序问题
        //--1.告诉Flink该使用EventTime来进行窗口计算
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);//新版本中不用设置,默认就是
        //--2.告诉Flink最大允许的延迟时间(或最大允许的乱序时间)
        SingleOutputStreamOperator<Order> watermarkDS = orderDS.assignTimestampsAndWatermarks(WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                //--3.哪一列是事件时间(Flink就可以自动计算Watermark了)
               .withTimestampAssigner((order, time) -> order.getEventTime()));
                /*.withTimestampAssigner(new SerializableTimestampAssigner<Order>() {
                    @Override
                    public long extractTimestamp(Order order, long recordTimestamp) {
                        return order.getEventTime();
                    }
                }));*/

        //--4.窗口计算
        SingleOutputStreamOperator<Order> result = watermarkDS.keyBy(Order::getUserId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum("money");

        //TODO 4.sink
        result.print();
        //TODO 5.execution
        env.execute();
    }


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Order {
        private String orderId;
        private Integer userId;
        private Integer money;
        private Long eventTime;
    }
}
