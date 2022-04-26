package cn.itcast.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * Author itcast
 * Desc
 * 使用FlinkSQL连接hive
 */
public class FlinkSQLDemo05 {
    public static void main(String[] args) throws Exception {
        //TODO 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        //TODO 2.定义HiveHiveCatalog
        String name            = "myhive";
        String defaultDatabase = "default";
        String hiveConfDir     = "./conf";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tenv.registerCatalog("myhive", hive);

        tenv.useCatalog("myhive");

        //TODO 3.执行sql
        String insertSQL = "insert into person select * from person";
        TableResult result = tenv.executeSql(insertSQL);

        //TODO 4.sink
        while (true){
            System.out.println(result.getJobClient().get().getJobStatus());
            Thread.sleep(1000);
        }

        //TODO 5.execute
        //env.execute();
    }

}
