package cn.itcast.source;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;
import java.util.UUID;

/**
 * Author itcast
 * Desc 演示Flink-DataStream-API-自定义Source
 * SourceFunction:非并行数据源(并行度只能=1)
 * RichSourceFunction:多功能非并行数据源(并行度只能=1)
 * ParallelSourceFunction:并行数据源(并行度能够>=1)
 * RichParallelSourceFunction:多功能并行数据源(并行度能够>=1)--后续学习的Kafka数据源使用的就是该接口
 */
public class SourceDemo04 {
    public static void main(String[] args) throws Exception {
        //TODO 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //TODO 2.source
        DataStreamSource<Order> ds = env.addSource(new MyOrderSource()).setParallelism(2);
        //TODO 3.transformation
        //TODO 4.sink
        ds.print();
        //TODO 5.execution
        env.execute();
    }
    /*
    每隔1秒随机生成一条订单信息(订单ID、用户ID、订单金额、时间戳)
    要求:
    - 随机生成订单ID(UUID)
    - 随机生成用户ID(0-2)
    - 随机生成订单金额(0-100)
    - 时间戳为当前系统时间
    SourceFunction:非并行数据源(并行度只能=1)
    RichSourceFunction:多功能非并行数据源(并行度只能=1)
    ParallelSourceFunction:并行数据源(并行度能够>=1)
    RichParallelSourceFunction:多功能并行数据源(并行度能够>=1)--后续学习的Kafka数据源使用的就是该接口
     */
   /* public static class MyOrderSource implements SourceFunction<Order> {
        @Override
        public void run(SourceContext<Order> ctx) throws Exception {

        }
        @Override
        public void cancel() {

        }
    }*/
   /* public static class MyOrderSource extends RichSourceFunction<Order> {
        @Override
        public void run(SourceContext<Order> ctx) throws Exception {

        }
        @Override
        public void cancel() {

        }
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }

        @Override
        public void close() throws Exception {
            super.close();
        }
    }*/
    /*public static class MyOrderSource implements ParallelSourceFunction<Order> {
        @Override
        public void run(SourceContext<Order> ctx) throws Exception {

        }
        @Override
        public void cancel() {

        }
    }*/
    public static class MyOrderSource extends RichParallelSourceFunction<Order> {
        private Boolean flag = true;
        //编写开启资源的代码
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }
        //要一直执行,不断生成数据
        @Override
        public void run(SourceContext<Order> ctx) throws Exception {
            Random ran = new Random();
            while (flag){
                String oid = UUID.randomUUID().toString();
                int uid = ran.nextInt(2);
                int money = ran.nextInt(100);
                long time = System.currentTimeMillis();
                Order order = new Order(oid, uid, money, time);
                ctx.collect(order);
                Thread.sleep(1000);
            }
        }
        //执行cancel命令时会执行(后面演示)
        @Override
        public void cancel() {
            flag = false;
        }
        //编写关闭资源的代码
        @Override
        public void close() throws Exception {
            super.close();
        }
    }
    //下面的注解是lombok提供的,可以自动生成get/set/构造/toString等方法
    //该注解生效的前提:
    //1.idea安装了lombok插件(首次安装后得重启)
    //2.项目pom中导入了依赖
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Order {
        private String id;
        private Integer userId;
        private Integer money;
        private Long createTime;
    }
}
