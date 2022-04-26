package cn.itcast.itcast.sink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Author itcast
 * Desc 演示Flink-DataStream-API-Sink
 */
public class SinkDemo01 {
    public static void main(String[] args) throws Exception {
        //TODO 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //TODO 2.source
        DataStream<String> ds = env.readTextFile("data/input/words.txt");
        //TODO 3.transformation
        //TODO 4.sink
        //1.ds.print 直接输出到控制台
        ds.print();
        ds.print("提示符");
        //2.ds.printToErr() 直接输出到控制台,用红色
        ds.printToErr();
        //3.ds.writeAsText("本地/HDFS的path",WriteMode.OVERWRITE).setParallelism(1)
        ds.writeAsText("data/output/result1").setParallelism(1);//以一个并行度写,生成一个文件
        ds.writeAsText("data/output/result2").setParallelism(2);//以多个并行度写,生成多个文件
        //在输出到path的时候,可以在前面设置并行度,如果
        //并行度>1,则path为目录
        //并行度=1,则path为文件名

        //TODO 5.execution
        env.execute();
    }
}
