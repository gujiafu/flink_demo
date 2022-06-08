package cn.itcast.cdc.sqlserver;

import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.connectors.sqlserver.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 1、数据库 TestDB 开启cdc 功能：
 * USE TestDB;
 * EXEC sys.sp_cdc_enable_db;
 *
 * 2、检查 数据库 TestDB 是否开启 cdc
 * select * from sys.databases where is_cdc_enabled=1;
 *
 * 3、库:TestDB, schema:dbo, 表:Inventory 开启cdc 功能：
 * EXEC sys.sp_cdc_enable_table @source_schema ='dbo', @source_name = 'Inventory', @role_name = NULL, @supports_net_changes = 0;
 *
 * 4、检查表 Inventory 是否开启 cdc
 * USE TestDB;
 * EXEC sys.sp_cdc_help_change_data_capture；
 */
public class CdcSqlserverSimpleDemo {

    public static void main(String[] args) throws Exception {
    DebeziumSourceFunction<String> sourceFunction =
        SqlServerSource.<String>builder()
            .hostname("192.168.88.166")
            .port(1433)
            .database("TestDB")
            .tableList("dbo.Inventory")
            .username("SA")
            .password("SqlServer2017")
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
