package cn.itcast.cdc.oracle;

import com.ververica.cdc.connectors.oracle.OracleSource;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *
 */
public class CdcOracleSimpleDemo {

    public static void main(String[] args) throws Exception {
    DebeziumSourceFunction<String> sourceFunction =
        OracleSource.<String>builder()
            .hostname("192.168.88.166")
            .port(1521)
            .database("SCOTT")
            .tableList("EMP")
            .username("flinkx")
            .password("flinkx")
//            .debeziumProperties(dbzProperties)
//            .startupOptions(StartupOptions.latest())
            .deserializer(new JsonDebeziumDeserializationSchema())
            .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(sourceFunction)
                .print()
                .setParallelism(1);

        env.execute();
    }
}
