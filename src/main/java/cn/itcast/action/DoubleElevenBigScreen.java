package cn.itcast.action;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.PriorityQueue;
import java.util.Random;

/**
 * Author itcast
 * Desc今天我们就做一个最简单的模拟电商统计大屏的小例子，
 * 需求如下：
 * 1.实时计算出当天零点截止到当前时间的销售总额
 * 2.计算出各个分类的销售额最大的top3
 * 3.每秒钟更新一次统计结果
 */
public class DoubleElevenBigScreen {
    public static void main(String[] args) throws Exception {
        //TODO 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //TODO 2.source
        //订单数据Tuple2<分类, 金额>
        DataStream<Tuple2<String, Double>> orderDS = env.addSource(new MySource());
        //TODO 3.transformation
        //-1.每秒预聚合各个分类的销售总额:从当天0点开始截止到目前为止的各个分类的销售总额
        SingleOutputStreamOperator<CategoryPojo> aggregateResult = orderDS.keyBy(t -> t.f0)
                //注意:下面的窗口表示每隔1天计算最近1天的数据,
                //.window(TumblingProcessingTimeWindows.of(Time.days(1)));
                //注意:下面的窗口表示每隔1s计算最近1天的数据(如果现在是1点,那么计算的是昨天1点到现在1点的1天的数据,而不是当天0点开始截止到目前为止的数据)
                //.window(SlidingProcessingTimeWindows.of(Time.days(1), Time.seconds(1)));
                //注意:中国使用UTC+08:00，您需要一天大小的时间窗口，
                //窗口从当地时间的每00:00:00开始，您可以使用{@code of（time.days（1），time.hours（-8））}
                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                //注意:下面表示每秒触发计算
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)))
                //聚合(可以使用之前学习的简单聚合:sum/reduce/或自定义聚合:apply或使用aggregate聚合(可以指定如何聚合及如何收集聚合结果))
                .aggregate(new MyAggregate(), new MyWindow());
        //输出查看下预聚合的结果
        //aggregateResult.print();
        //按照分类将订单金额进行聚合:
        //分类名称  金额  时间
        //男装      100   2021-11-11 11:11:11
        //女装      100   2021-11-11 11:11:11

        //男装      200   2021-11-11 11:11:12
        //女装      200   2021-11-11 11:11:12

        //-2.计算所有分类的销售总额和分类销售额最大Top3
        //要是每秒更新/计算所有分类目前的销售总额和分类销售额Top3
        //aggregateResult.keyBy(CategoryPojo::getDateTime)
        aggregateResult.keyBy(c -> c.getDateTime())//先按照时间对数据分组,因为后续要每秒更新/计算销售总额和分类销售额Top3
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                //.apply(new WindowFunction<CategoryPojo, Object, String, TimeWindow>() {})
                .process(new MyProcessWindowFunction());

        //TODO 4.sink
        //TODO 5.execute
        env.execute();
    }

    /**
     * abstract class ProcessWindowFunction<IN, OUT, KEY, W extends Window>
     */
    public static class MyProcessWindowFunction extends ProcessWindowFunction<CategoryPojo, Object, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<CategoryPojo> categoryPojos, Collector<Object> out) throws Exception {
            Double totalAmount = 0d;//用来记录销售总额
            //用大小顶堆来计算TopN
            //用大顶堆(大的数据在堆顶,取最小的TopN时使用)还是小顶堆(小的数据在堆顶,取最大的TopN时使用)?
            //2--堆顶
            //3
            //4--堆底
            //5进来,比堆顶大,堆顶元素移除,5下沉
            //3
            //4
            //5
            //1进来,比堆顶小,原来不变
            //3
            //4
            //5
            //100进来,比堆顶大,堆顶元素移除,100下沉
            //4
            //5
            //100
            //注意:Java里面提供了一个优先级队列PriorityQueue实现了大小顶堆的功能
            //https://blog.csdn.net/hefenglian/article/details/81807527
            //所以创建一个长度为3的PriorityQueue,并指定比较规则(正常的比较规则:元素1>=元素2 ? 1:-1,就是小顶堆)
            PriorityQueue<CategoryPojo> queue = new PriorityQueue<>(3, (c1, c2) -> c1.getTotalPrice() >= c2.getTotalPrice() ? 1 : -1);

            for (CategoryPojo categoryPojo : categoryPojos) {
                //--1.计算截止到目前为止的所有分类的销售总额
                totalAmount += categoryPojo.getTotalPrice();
                //--2.分类销售额最大Top3
                if (queue.size() < 3) {
                    queue.add(categoryPojo);
                } else {//queue.size() >= 3
                    //查看堆顶元素,但不移除
                    CategoryPojo peek = queue.peek();
                    if (categoryPojo.getTotalPrice() > peek.getTotalPrice()) {
                        //进来的元素比堆顶大
                        //堆顶元素移除,进来的元素下沉
                        //queue.remove(peek);
                        queue.poll();
                        queue.add(categoryPojo);
                    }/*else{//进来的元素比堆顶小,不用变

                    }*/
                }
            }

            //--3.直接在这里输出
            System.out.println("================================================================================================================================");
            System.out.println("----当前时间:----");
            System.out.println(key);
            System.out.println("----销售总额:----");
            System.out.println(new BigDecimal(totalAmount).setScale(2, RoundingMode.HALF_UP));
            System.out.println("----销售额Top3分类:----");
            queue.stream()
                    .map(c -> {
                        c.setTotalPrice(new BigDecimal(c.getTotalPrice()).setScale(2, RoundingMode.HALF_UP).doubleValue());
                        return c;
                    })
                    .sorted((c1, c2) -> c1.getTotalPrice() <= c2.getTotalPrice() ? 1 : -1)
                    .forEach(System.out::println);
        }
    }

    /**
     * interface AggregateFunction<IN, ACC, OUT>
     * 自定义聚合函数,实现各个分类销售额的预聚合/累加
     */
    public static class MyAggregate implements AggregateFunction<Tuple2<String, Double>, Double, Double> {
        //初始化累加器
        @Override
        public Double createAccumulator() {
            return 0d;
        }

        //将数据聚合/累加到累加器上
        @Override
        public Double add(Tuple2<String, Double> value, Double accumulator) {
            return value.f1 + accumulator;
        }

        //获取累加结果
        @Override
        public Double getResult(Double accumulator) {
            return accumulator;
        }

        //合并结果(和历史结果合并)
        @Override
        public Double merge(Double a, Double b) {
            return a + b;
        }
    }


    /**
     * interface WindowFunction<IN, OUT, KEY, W extends Window>
     * 自定义窗口函数,实现窗口聚合数据的收集
     */
    public static class MyWindow implements WindowFunction<Double, CategoryPojo, String, TimeWindow> {
        private FastDateFormat df = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

        @Override
        public void apply(String key, TimeWindow window, Iterable<Double> input, Collector<CategoryPojo> out) throws Exception {
            double totalPrice = 0d;
            for (Double price : input) {
                totalPrice += price;
            }
            //System.out.println(df.format(window.getStart())+"-----"+df.format(window.getEnd()));
            CategoryPojo categoryPojo = new CategoryPojo();
            categoryPojo.setCategory(key);
            categoryPojo.setDateTime(df.format(System.currentTimeMillis()));
            categoryPojo.setTotalPrice(totalPrice);
            out.collect(categoryPojo);
        }
    }

    /**
     * 用于存储聚合的结果
     */
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CategoryPojo {
        private String category;//分类名称
        private double totalPrice;//该分类总销售额
        private String dateTime;// 截止到当前时间的时间,本来应该是EventTime,但是我们这里简化了直接用当前系统时间即可
    }

    /**
     * 自定义数据源实时产生订单数据Tuple2<分类, 金额>
     */
    public static class MySource implements SourceFunction<Tuple2<String, Double>> {
        private boolean flag = true;
        private String[] categorys = {"女装", "男装", "图书", "家电", "洗护", "美妆", "运动", "游戏", "户外", "家具", "乐器", "办公"};
        private Random random = new Random();

        @Override
        public void run(SourceContext<Tuple2<String, Double>> ctx) throws Exception {
            while (flag) {
                //随机生成分类和金额
                int index = random.nextInt(categorys.length);//[0~length) ==> [0~length-1]
                String category = categorys[index];//获取的随机分类
                double price = random.nextDouble() * 100;//注意nextDouble生成的是[0~1)之间的随机数,*100之后表示[0~100)
                ctx.collect(Tuple2.of(category, price));
                Thread.sleep(20);
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }
}
