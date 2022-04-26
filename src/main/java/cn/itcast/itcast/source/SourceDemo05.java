package cn.itcast.itcast.source;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Author itcast
 * Desc 演示Flink-DataStream-API-自定义Source-MySQL
 * SourceFunction:非并行数据源(并行度只能=1)
 * RichSourceFunction:多功能非并行数据源(并行度只能=1)
 * ParallelSourceFunction:并行数据源(并行度能够>=1)
 * RichParallelSourceFunction:多功能并行数据源(并行度能够>=1)--后续学习的Kafka数据源使用的就是该接口
 */
public class SourceDemo05 {
    public static void main(String[] args) throws Exception {
        //TODO 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //TODO 2.source
        DataStream<Student> ds = env.addSource(new MySQLSource()).setParallelism(1);
        //TODO 3.transformation
        //TODO 4.sink
        ds.print();
        //TODO 5.execution
        env.execute();
    }

    /*
    使用自定义Source加载MySQL中的最新数据,每隔2s加载一次
    SourceFunction:非并行数据源(并行度只能=1)
    RichSourceFunction:多功能非并行数据源(并行度只能=1)
    ParallelSourceFunction:并行数据源(并行度能够>=1)
    RichParallelSourceFunction:多功能并行数据源(并行度能够>=1)--后续学习的Kafka数据源使用的就是该接口
     */
    public static class MySQLSource extends RichParallelSourceFunction<Student> {
        private Boolean flag = true;
        private Connection conn;
        private PreparedStatement ps;

        //开启连接--只会执行一次
        @Override
        public void open(Configuration parameters) throws Exception {
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata", "root", "root");
            ps = conn.prepareStatement("select id,name,age from t_student");
        }

        //查询数据,要一直执行,每隔2s查一次最新的数据
        @Override
        public void run(SourceContext<Student> ctx) throws Exception {
            while (flag) {
                ResultSet rs = ps.executeQuery();
                while (rs.next()){
                    int id = rs.getInt("id");
                    String name = rs.getString("name");
                    int age = rs.getInt("age");
                    ctx.collect(new Student(id,name,age));
                }
                Thread.sleep(2000);
            }
        }

        //执行cancel命令时执行
        @Override
        public void cancel() {
            flag = false;
        }

        //关闭连接--只会执行一次
        @Override
        public void close() throws Exception {
           if(conn!=null) conn.close();
           if(ps!=null) ps.close();
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Student {
        private Integer id;
        private String name;
        private Integer age;
    }
}
