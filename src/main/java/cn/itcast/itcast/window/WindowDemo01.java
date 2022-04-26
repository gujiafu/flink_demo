package cn.itcast.itcast.window;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Author itcast
 * Desc 演示Flink-Window-基于时间的滑动和滚动窗口
 * 需求:
 * nc -lk 9999
 * 有如下数据表示:
 * 信号灯编号和通过该信号灯的车的数量
 * 9,3
 * 9,2
 * 9,7
 * 4,9
 * 2,6
 * 1,5
 * 2,3
 * 5,7
 * 5,4
 * 需求1:每5秒钟统计一次，最近5秒钟内，各个路口通过红绿灯汽车的数量--基于时间的滚动窗口
 * 需求2:每5秒钟统计一次，最近10秒钟内，各个路口通过红绿灯汽车的数量--基于时间的滑动窗口
 */
public class WindowDemo01 {
    public static void main(String[] args) throws Exception {
        //TODO 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //TODO 2.source
        DataStream<String> ds = env.socketTextStream("node1", 9999);
        //TODO 3.transformation
        //将每一行数据封装为JavaBean
        DataStream<CartInfo> cartInfoDS = ds.map(new MapFunction<String, CartInfo>() {
            @Override
            public CartInfo map(String value) throws Exception {
                String[] arr = value.split(",");
                return new CartInfo(arr[0], Integer.parseInt(arr[1]));
            }
        });
        //后面的需求要求各个路口(信号灯)的数据,所以需要先分组
        /*cartInfoDS.keyBy(new KeySelector<CartInfo, String>() {
            @Override
            public String getKey(CartInfo value) throws Exception {
                return value.getSensorId();
            }
        });*/
        //cartInfoDS.keyBy("sensorId");
        //cartInfoDS.keyBy(car->car.getSensorId());//lambda表达式
        KeyedStream<CartInfo, String> keyedDS = cartInfoDS.keyBy(CartInfo::getSensorId);//表示使用lambda表达式的变形(方法引用),本质就是方法可以转为函数

        //需求1:每5秒钟统计一次，最近5秒钟内，各个路口通过红绿灯汽车的数量--基于时间的滚动窗口
        DataStream<CartInfo> result1 = keyedDS.window(TumblingProcessingTimeWindows.of(Time.seconds(5))).sum("count");

        //需求2:每5秒钟统计一次，最近10秒钟内，各个路口通过红绿灯汽车的数量--基于时间的滑动窗口
        //size The size of the generated windows.--窗口大小
        //slide The slide interval of the generated windows.--滑动间隔
        DataStream<CartInfo> result2 = keyedDS.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5))).sum("count");


        //TODO 4.sink
        //1,5
        //2,5
        //1,5
        //2,5
        //1,5
        //2,5
        //result1.print();
        result2.print();

        //TODO 5.execution
        env.execute();
    }
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CartInfo {
        private String sensorId;//信号灯id
        private Integer count;//通过该信号灯的车的数量
    }
}
