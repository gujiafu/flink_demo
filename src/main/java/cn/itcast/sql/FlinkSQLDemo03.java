package cn.itcast.sql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * Author itcast
 * Desc
 * 使用Flink SQL来统计5秒内 每个用户的 订单总数、订单的最大金额、订单的最小金额
 * 也就是每隔5秒统计最近5秒的每个用户的订单总数、订单的最大金额、订单的最小金额
 * 那么接下来使用FlinkTable&SQL-API来实现
 * 要求使用基于事件时间的滚动窗口+Watermark水印机制来实现
 */
public class FlinkSQLDemo03 {
    public static void main(String[] args) throws Exception {
        //TODO 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        //TODO 2.source/准备表View/Table
        DataStream<Order> orderDS  = env.addSource(new SourceFunction<Order>() {
            private Boolean flag = true;
            @Override
            public void run(SourceContext<Order> ctx) throws Exception {
                Random random = new Random();
                while (flag) {
                    Order order = new Order(
                            UUID.randomUUID().toString(), //订单id
                            random.nextInt(3),//用户id
                            random.nextInt(101), //订单金额
                            System.currentTimeMillis());//创建时间
                    ctx.collect(order);
                    TimeUnit.SECONDS.sleep(1);
                }
            }
            @Override
            public void cancel() {
                flag = false;
            }
        });
        //TODO 3.transformation/查询SQL风格和Table风格(SQL/DSL)
        //--1.告诉Flink使用事件时间
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<Order> watermarkDS = orderDS
                //--2.告诉Flink最大允许的延迟/乱序时间
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                //--3.告诉Flink哪一列是事件时间
                .withTimestampAssigner((order, time) -> order.getCreateTime()));

        //watermarkDS.print();

        //DataStream-->View
        //注意:告诉FlinkSQL哪一列是时间
        tenv.createTemporaryView("t_order1",watermarkDS,$("orderId"),$("userId"),$("money"),$("createTime").rowtime());
        //DataStream-->Table
        Table t_order2 = tenv.fromDataStream(watermarkDS, $("orderId"), $("userId"), $("money"), $("createTime").rowtime());

/*
select userId,count(orderId) as orderCounts,max(money) as maxMoney,min(money) as minMoney
from t_order1 group by userId,TUMBLE(createTime, INTERVAL '5' SECOND)
  */
        Table resultTable1 = tenv.sqlQuery(
                "select userId,count(orderId) as orderCounts,max(money) as maxMoney,min(money) as minMoney\n" +
                "from t_order1 group by userId,TUMBLE(createTime, INTERVAL '5' SECOND)"
        );
        //https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/tableApi.html
        Table resultTable2 = t_order2.window(Tumble.over(lit(5).seconds()).on($("createTime")).as("myWindow"))
                .groupBy($("userId"),$("myWindow"))
                .select(
                        $("userId"),
                        $("orderId").count().as("orderCounts"),
                        $("money").max().as("maxMoney"),
                        $("money").min().as("minMoney"));


        //TODO 4.sink
        DataStream<Tuple2<Boolean, Row>> result1 = tenv.toRetractStream(resultTable1, Row.class);
        DataStream<Tuple2<Boolean, Row>> result2 = tenv.toRetractStream(resultTable2, Row.class);

        result1.print();
        result2.print();

        //TODO 5.execute
        env.execute();
    }
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Order {
        private String orderId;
        private Integer userId;
        private Integer money;
        private Long createTime;//注意不要使用eventtime作为字段名,因为是flinkSQL关键字
    }
}
