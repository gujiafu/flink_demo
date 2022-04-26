package cn.itcast.itcast.hello;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Author itcast
 * Desc 演示Flink-DataStream-API完成批处理WordCount--OnYarn
 * 注意:在flink1.12的时候DataStream既支持批处理也支持流处理
 */
public class WordCount4 {
    public static void main(String[] args) throws Exception {
        //String path = args[0]; //让用户指定文件输出路径
        String path = "hdfs://node1:8020/wordcount/output422_default_";
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        if (parameterTool.has("output")){
            path = parameterTool.get("output");
        }
        path = path + System.currentTimeMillis();
        //TODO 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //不设置模式是流模式
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);//自动
        //env.setRuntimeMode(RuntimeExecutionMode.STREAMING);//流
        //env.setRuntimeMode(RuntimeExecutionMode.BATCH);//批
        //如果不知道该用什么模式就用AUTOMATIC自动
        //TODO 2.source
        DataStream<String> linesDS = env.fromElements(
                "itcast hadoop spark",
                "itcast hadoop spark",
                "itcast hadoop",
                "itcast");
        //TODO 3.transformation
        //每一行按照空格切分并压扁
        DataStream<String> wordsDS = linesDS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });
        //每个单词记为1
        DataStream<Tuple2<String, Integer>> wordAndOneDS = wordsDS.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        });
        //分组
        //注意:在DataSet中分组用groupBy,在DataStream中分组用keyBy
        //KeyedStream<Tuple2<String, Integer>, Tuple> groupedDS = wordAndOneDS.keyBy(0);
        KeyedStream<Tuple2<String, Integer>, String> groupedDS = wordAndOneDS.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });
        //聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = groupedDS.sum(1);
        //TODO 4.sink
        //result.print();
        //写入HDFS如果存在权限问题:
        //进行如下设置:
        //hadoop fs -chmod -R 777  /
        //并在代码中添加:
        System.setProperty("HADOOP_USER_NAME", "root");
        result.writeAsText(path).setParallelism(1);//表示往path中写入结果(以单个文件形式)
        //TODO 5.execute
        //注意:在DataStream中execute不能省略
        env.execute();
    }
}
// /export/server/flink/bin/flink run -m yarn-cluster -yjm 1024 -ytm 1024 -c cn.itcast.hello.WordCount4 /root/wc.jar --output hdfs://node1:8020/wordcount/output422_my_
