package cn.itcast.action;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

/**
 * Author itcast
 * Desc
 * 在电商领域会有这么一个场景，如果用户买了商品，在订单完成之后，一定时间之内没有做出评价，系统自动给与五星好评(或者下单之后在一定时间内没有付款, 就触发站内信/短信提醒/取消...)
 * 我们今天主要使用Flink的定时器来简单实现这一功能。
 * 注意: 这个需求不使用大数据的技术,就是用Web的定时器也可以做
 * 课后可以用你熟悉的编程语言/工具/框架去实现
 */
public class OrderAutomaticFavorite {
    public static void main(String[] args) throws Exception {
        //TODO 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //TODO 2.source
        //Tuple3<用户id,订单id,订单生成时间>
        DataStream<Tuple3<String, String, Long>> orderDS = env.addSource(new MySource());
        //TODO 3.transformation
        //设置经过interval毫秒用户未对订单做出评价就自动给予好评,为了方便测试,设置5000ms/5s(实际中可以长一点)
        long interval = 5000L;

        //实现这个功能原本不需要分组,但是为了后面使用keyedState状态,所以这里分下组
        orderDS.keyBy(t -> t.f0)
                .process(new MyKeyedProcessFunction(interval));
        //TODO 4.sink
        //TODO 5.execute
        env.execute();
    }

    /**
     * public abstract class KeyedProcessFunction<K, I, O>
     */

    public static class MyKeyedProcessFunction extends KeyedProcessFunction<String, Tuple3<String, String, Long>, Object> {
        private long interval = 0L;

        public MyKeyedProcessFunction(long interval) {
            this.interval = interval;
        }

        //准备一个MapState存储订单信息<订单号,订单时间>
        private MapState<String, Long> mapState = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            //创建状态描述器
            MapStateDescriptor<String, Long> descriptor = new MapStateDescriptor<>("mapState", String.class, Long.class);
            //根据状态描述器初始化状态
            mapState = getRuntimeContext().getMapState(descriptor);
        }

        //处理进来的每个元素/订单,然后注册定时器,到时候判断是否进行了好评
        @Override
        public void processElement(Tuple3<String, String, Long> value, Context ctx, Collector<Object> out) throws Exception {
            //把订单信息存入状态中方便后续使用
            mapState.put(value.f1, value.f2);

            //注册定时器在interval时间后执行/在value.f2 + interval时间时执行
            ctx.timerService().registerProcessingTimeTimer(value.f2 + interval);
        }

        //实现定时器执行方法
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Object> out) throws Exception {
            //定时器触发的时候需要检查状态中的订单是否已经好评了
            Iterator<Map.Entry<String, Long>> iterator = mapState.iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Long> entry = iterator.next();
                String orderId = entry.getKey();
                Long orderTime = entry.getValue();
                //判断该订单是否已经评价--实际中需要调用外部订单系统的接口,我们自己简单起见直接调用模拟的方法
                if (isEvaluate(orderId)) {
                    //已经评价过了
                    System.out.println("该订单:" + orderId + "用户已评价");
                    //移除当前订单
                    iterator.remove();//迭代器可以直接移除元素
                    //保险一定状态中也移除
                    mapState.remove(orderId);
                } else {
                    //没有评价
                    //注意:一个key(用户)有很多订单,有的可能超时,有的可能还未超时
                    //所以需要判断是否超时
                    if (System.currentTimeMillis() - orderTime >= interval) {
                        //超时且未评价,需要系统给予自动好评
                        System.out.println("该订单:" + orderId + "已超时未评价,系统给予自动好评");
                        //移除当前订单
                        iterator.remove();//迭代器可以直接移除元素
                        //保险一定状态中也移除
                        mapState.remove(orderId);

                    }/*else{
                        //未超时,不用管

                    }*/
                }
            }
        }

        //模拟订单系统,传入订单id,返回该订单是否已经评价
        public boolean isEvaluate(String orderId) {
            //下面这行代码会随机返回订单是否已经评价
            return new Random().nextInt(10) % 2 == 0;
        }
    }

    /**
     * 自定义source实时产生订单数据Tuple3<用户id,订单id,订单生成时间>
     */
    public static class MySource implements SourceFunction<Tuple3<String, String, Long>> {
        private boolean flag = true;

        @Override
        public void run(SourceContext<Tuple3<String, String, Long>> ctx) throws Exception {
            Random random = new Random();
            while (flag) {
                String userId = random.nextInt(5) + "";
                String orderId = UUID.randomUUID().toString();
                long currentTimeMillis = System.currentTimeMillis();
                ctx.collect(Tuple3.of(userId, orderId, currentTimeMillis));
                Thread.sleep(500);
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }
}
