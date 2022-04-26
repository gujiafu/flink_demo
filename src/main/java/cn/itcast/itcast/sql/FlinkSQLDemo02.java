package cn.itcast.itcast.sql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Author itcast
 * Desc 使用SQL和Table(SQL和DSL)两种编程风格对DataStream中的单词进行统计
 */
public class FlinkSQLDemo02 {
    public static void main(String[] args) throws Exception {
        //TODO 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        //TODO 2.source/准备表View/Table
        DataStream<WC> wordsDS = env.fromElements(
                new WC("Hello", 1),
                new WC("World", 1),
                new WC("Hello", 1)
        );

        //TODO 3.transformation/查询SQL风格和Table风格(SQL/DSL)
        //DataStream-->View
        tenv.createTemporaryView("table1",wordsDS,$("word"),$("frequency"));
        //DataStream-->Table
        Table table2 = tenv.fromDataStream(wordsDS, $("word"), $("frequency"));

        //查询-编程风格1-SQL
        Table resultTable1 = tenv.sqlQuery("select word,sum(frequency) as frequency from table1 group by word");

        //查询-编程风格2-Table(DSL)
        Table resultTable2 = table2.groupBy($("word"))
                .select($("word"), $("frequency").sum().as("frequency")).filter($("frequency").isEqual(2));

        //TODO 4.sink
        //Table-->DataStream
        DataStream<Tuple2<Boolean, Row>> result1 = tenv.toRetractStream(resultTable1, Row.class);
        DataStream<Tuple2<Boolean, WC>> result2 = tenv.toRetractStream(resultTable2, WC.class);

        result1.print("result1");
        //result2.print("result2");

        //TODO 5.execute
        env.execute();
    }
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WC {
        public String word;
        public long frequency;
    }
}
