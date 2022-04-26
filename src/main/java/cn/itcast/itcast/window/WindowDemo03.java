package cn.itcast.itcast.window;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
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
 * 设置会话超时时间为10s,10s内没有数据到来则触发计算上一个窗口的数据
 */
public class WindowDemo03 {
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

        //设置会话超时时间为10s,10s内没有数据到来则触发计算上一个窗口的数据(前提是窗口中得有数据)
        DataStream<CartInfo> result = keyedDS.window(ProcessingTimeSessionWindows.withGap(Time.seconds(10))).sum("count");


        //TODO 4.sink
        //1,5
        //2,5
        result.print();

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
