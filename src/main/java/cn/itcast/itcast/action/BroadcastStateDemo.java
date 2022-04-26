package cn.itcast.itcast.action;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Author itcast
 * Desc 需求:
 * 1.实时日志事件流:<userID, eventTime, eventType, productID> 哪个用户 在 什么时间,对 哪个商品 进行了 什么操作
 * 2.用户信息流(配置流/规则流): <用户id,<姓名,年龄>> 用户的详细信息
 * 3.将较小的信息流(配置流/规则流)作为状态广播到各个节点,便于对实时日志事件流中的用户信息进行补全!--其实就是做 状态广播 并要支持状态更新
 */
public class BroadcastStateDemo {
    public static void main(String[] args) throws Exception {
        //TODO 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //TODO 2.source
        DataStream<Tuple4<String, String, String, Integer>> logDS = env.addSource(new MySource());
        DataStream<Map<String, Tuple2<String, Integer>>> userInfoDS = env.addSource(new MySQLSource());

        //TODO 3.transformation
        //--1.定义状态描述器(要将userInfoDS作为状态进行广播,key可以指定其他值,也可以不需要key/void/null)
        //下面的数据结构较为复杂,可以简化,这里只是给大家演示一下复杂嵌套类型的声明而已
        MapStateDescriptor<Void, Map<String, Tuple2<String, Integer>>> descriptor = new MapStateDescriptor<>("user", Types.VOID, Types.MAP(Types.STRING, Types.TUPLE(Types.STRING, Types.INT)));

        //--2.根据状态描述器将userInfoDS作为状态进行广播
        BroadcastStream<Map<String, Tuple2<String, Integer>>> broadcastDS = userInfoDS.broadcast(descriptor);

        //--3.将实时日志事件流和广播流进行连接
        BroadcastConnectedStream<Tuple4<String, String, String, Integer>, Map<String, Tuple2<String, Integer>>> connectDS = logDS.connect(broadcastDS);

        //--4.处理连接流中的数据
        SingleOutputStreamOperator<Object> resultDS = connectDS.process(new BroadcastProcessFunction<
                Tuple4<String, String, String, Integer>, //实时日志事件流:<userID, eventTime, eventType, productID> 哪个用户 在 什么时间,对 哪个商品 进行了 什么操作
                Map<String, Tuple2<String, Integer>>, //用户信息流(配置流/规则流): <用户id,<姓名,年龄>> 用户的详细信息
                Object>() {
            //处理元素
            @Override
            public void processElement(Tuple4<String, String, String, Integer> value, ReadOnlyContext ctx, Collector<Object> out) throws Exception {
                String userId = value.f0;
                //拿到状态
                ReadOnlyBroadcastState<Void, Map<String, Tuple2<String, Integer>>> broadcastState = ctx.getBroadcastState(descriptor);
                Map<String, Tuple2<String, Integer>> userMap = broadcastState.get(null);
                if (userMap != null) {
                    Tuple2<String, Integer> user = userMap.get(userId);
                    String name = user.f0;
                    Integer age = user.f1;
                    out.collect(Tuple6.of(userId, name, age, value.f1, value.f3, value.f2));
                }
            }
            //处理广播状态
            @Override
            public void processBroadcastElement(Map<String, Tuple2<String, Integer>> value, Context ctx, Collector<Object> out) throws Exception {
                BroadcastState<Void, Map<String, Tuple2<String, Integer>>> broadcastState = ctx.getBroadcastState(descriptor);
                //清理广播数据后再重新广播新数据
                broadcastState.clear();
                broadcastState.put(null, value);
            }
        });

        //TODO 4.sink
        resultDS.print();

        //TODO 5.execute
        env.execute();
    }


    /**
     * 1.实时日志事件流:<userID, eventTime, eventType, productID> 哪个用户 在 什么时间,对 哪个商品 进行了 什么操作
     * <userID, eventTime, eventType, productID>
     */
    public static class MySource implements SourceFunction<Tuple4<String, String, String, Integer>> {
        private boolean isRunning = true;
        @Override
        public void run(SourceContext<Tuple4<String, String, String, Integer>> ctx) throws Exception {
            Random random = new Random();
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            while (isRunning){
                int id = random.nextInt(4) + 1;
                String user_id = "user_" + id;
                String eventTime = df.format(new Date());
                String eventType = "type_" + random.nextInt(3);
                int productId = random.nextInt(4);
                ctx.collect(Tuple4.of(user_id,eventTime,eventType,productId));
                Thread.sleep(500);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
    /**
     * 2.用户信息流(配置流/规则流): <用户id,<姓名,年龄>> 用户的详细信息
     * <用户id,<姓名,年龄>>
     */
    public static class MySQLSource extends RichSourceFunction<Map<String, Tuple2<String, Integer>>> {
        private boolean flag = true;
        private Connection conn = null;
        private PreparedStatement ps = null;
        private ResultSet rs = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata", "root", "root");
            String sql = "select `userID`, `userName`, `userAge` from `user_info`";
            ps = conn.prepareStatement(sql);
        }
        @Override
        public void run(SourceContext<Map<String, Tuple2<String, Integer>>> ctx) throws Exception {
            while (flag){
                Map<String, Tuple2<String, Integer>> map = new HashMap<>();
                ResultSet rs = ps.executeQuery();
                while (rs.next()){
                    String userID = rs.getString("userID");
                    String userName = rs.getString("userName");
                    int userAge = rs.getInt("userAge");
                    //Map<String, Tuple2<String, Integer>>
                    map.put(userID, Tuple2.of(userName,userAge));
                }
                ctx.collect(map);
                Thread.sleep(5000);//每隔5s更新一下用户的配置信息!
            }
        }
        @Override
        public void cancel() {
            flag = false;
        }
        @Override
        public void close() throws Exception {
            if (conn != null) conn.close();
            if (ps != null) ps.close();
            if (rs != null) rs.close();
        }
    }
}
