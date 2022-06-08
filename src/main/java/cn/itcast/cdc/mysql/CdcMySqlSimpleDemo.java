package cn.itcast.cdc.mysql;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/** Example Tests for {@link MySqlSource}. */
public class CdcMySqlSimpleDemo {

    public static void main(String[] args) throws Exception {
        MySqlSource<String> mySqlSource =
                MySqlSource.<String>builder()
                        .hostname("192.168.88.166")
                        .port(3306)
                        .databaseList("flink_cdc_db")
                        .tableList("flink_cdc_db.customers_1")
                        .username("root")
                        .password("123456")
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .includeSchemaChanges(true)
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000);
        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySqlParallelSource")
                .setParallelism(4)
                .print()
                .setParallelism(1);

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
