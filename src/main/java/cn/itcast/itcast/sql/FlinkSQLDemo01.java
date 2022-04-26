package cn.itcast.itcast.sql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Author itcast
 * Desc 将DataStream注册为Table和View并进行SQL统计
 */
public class FlinkSQLDemo01 {
    public static void main(String[] args) throws Exception {
        //TODO 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        //TODO 2.source/准备表View/Table
        DataStream<Order> orderA = env.fromCollection(Arrays.asList(
                new Order(1L, "beer", 3),
                new Order(1L, "diaper", 4),
                new Order(3L, "rubber", 2)));

        DataStream<Order> orderB = env.fromCollection(Arrays.asList(
                new Order(2L, "pen", 3),
                new Order(2L, "rubber", 3),
                new Order(4L, "beer", 1)));

        //TODO 3.transformation/查询SQL风格和Table风格(SQL/DSL)
        //将DataStream注册为View(tableA是表名)
        tenv.createTemporaryView("tableA",orderA,$("user"),$("product"),$("amount"));
        //将DataStream注册为Table(tableB是变量名)
        Table tableB = tenv.fromDataStream(orderB, $("user"), $("product"), $("amount"));
        tableB.printSchema();
        /*
        root
         |-- user: BIGINT
         |-- product: STRING
         |-- amount: INT
         */
        System.out.println(tableB);//UnnamedTable$0


        /*
select * from tableA
union all
select * from tableB
         */
        Table resultTable = tenv.sqlQuery("select * from tableA \n" +
                "union all \n" +
                "select * from "+tableB);

        //TODO 4.sink
        resultTable.printSchema();
        /*
        root
         |-- user: BIGINT
         |-- product: STRING
         |-- amount: INT
         */

        //转为DataStream再输出
        //DataStream<Order> result = tenv.toAppendStream(resultTable, Order.class);
        DataStream<Tuple2<Boolean, Order>> result = tenv.toRetractStream(resultTable, Order.class);
        result.print();

        //TODO 5.execute
        env.execute();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Order {
        public Long user;
        public String product;
        public int amount;
    }
}
