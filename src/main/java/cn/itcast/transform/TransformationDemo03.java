package cn.itcast.transform;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Author itcast
 * Desc 演示Flink-DataStream-API-Transformation
 * split、select(已经过期)和Side Outputs
 */
public class TransformationDemo03 {
    public static void main(String[] args) throws Exception {
        //TODO 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //TODO 2.source
        DataStream<Integer> ds = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        //TODO 3.transformation
        //使用拆分API将ds中的数据按照奇数偶数才出来(可以使用filter过滤2次,一次过滤奇数,一次过滤偶数,也可以使用如下API)

        /*//过期API
        SplitStream<Integer> splitResult = ds.split(new OutputSelector<Integer>() {
            @Override
            public Iterable<String> select(Integer value) {
                //value是进来的数字
                if (value % 2 == 0) {
                    //偶数
                    ArrayList<String> list = new ArrayList<>();
                    list.add("偶数");
                    return list;
                } else {
                    //奇数
                    ArrayList<String> list = new ArrayList<>();
                    list.add("奇数");
                    return list;
                }
            }
        });
        DataStream<Integer> evenResult = splitResult.select("偶数");
        DataStream<Integer> oddResult = splitResult.select("奇数");*/

        //现在应该用Side Outputs(侧道输出)
        //声明2个侧道输出标签
        OutputTag<Integer> evenTag = new OutputTag<>("偶数", TypeInformation.of(Integer.class));
        OutputTag<Integer> oddTag = new OutputTag<>("奇数", TypeInformation.of(Integer.class));
        //对流中的数据打上不同的标签
        SingleOutputStreamOperator<Integer> processDS = ds.process(new ProcessFunction<Integer, Integer>() {
            @Override
            public void processElement(Integer value, Context ctx, Collector<Integer> out) throws Exception {
                if (value % 2 == 0) {//偶数
                    ctx.output(evenTag, value);
                } else {//奇数
                    ctx.output(oddTag, value);
                }
                //out.collect(value);
            }
        });
        //从processDS取出打上不同标签的数据
        DataStream<Integer> evenDS = processDS.getSideOutput(evenTag);
        DataStream<Integer> oddDS = processDS.getSideOutput(oddTag);


        //TODO 4.sink
        evenDS.print("偶数");
        oddDS.print("奇数");
        //TODO 5.execution
        env.execute();
    }
}
