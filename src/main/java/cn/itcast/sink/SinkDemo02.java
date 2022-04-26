package cn.itcast.sink;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * Author itcast
 * Desc 演示Flink-DataStream-API-Sink
 */
public class SinkDemo02 {
    public static void main(String[] args) throws Exception {
        //TODO 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //TODO 2.source
        DataStream<Student> studentDS = env.fromElements(new Student(null, "tony", 18));
        //TODO 3.transformation
        //TODO 4.sink
        studentDS.addSink(new RichSinkFunction<Student>() {
            private Connection conn;
            private PreparedStatement ps;

            //开启连接
            @Override
            public void open(Configuration parameters) throws Exception {
                conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata", "root", "root");
                ps = conn.prepareStatement("INSERT INTO `t_student` (`id`, `name`, `age`) VALUES (null, ?, ?);");
            }
            //执行插入操作
            @Override
            public void invoke(Student value, Context context) throws Exception {
                //设置?占位符
                ps.setString(1,value.getName());
                ps.setInt(2,value.getAge());
                //执行
                ps.executeUpdate();
            }
            //关闭连接
            @Override
            public void close() throws Exception {
                if(conn!=null) conn.close();
                if(ps!=null) ps.close();
            }
        });

        //TODO 5.execution
        env.execute();
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
