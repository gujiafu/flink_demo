package cep;

import cn.itcast.cep.bean.LoginUser;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

public class LoginFailTest {

  public static void main(String[] args) throws Exception {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//      env.setParallelism(1);
      KeyedStream<LoginUser, Integer> source = env.fromElements(new LoginUser(1, "192.168.0.1", "fail", 1558430842000L),        //2019-05-21 17:27:22
              new LoginUser(1, "192.168.0.2", "fail", 1558430843000L),        //2019-05-21 17:27:23
              new LoginUser(1, "192.168.0.3", "fail", 1558430844000L),        //2019-05-21 17:27:24
              new LoginUser(1, "192.168.0.4", "fail", 1558430847000L),        //2019-05-21 17:27:27
              new LoginUser(2, "192.168.10.10", "success", 1558430845000L))    //2019-05-21 17:27:25)
              .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginUser>(Time.seconds(0)) {
                  @Override
                  public long extractTimestamp(LoginUser element) {
                      return element.getEventTime();
                  }
              }).keyBy(LoginUser::getId);

      Pattern<LoginUser, LoginUser> pattern = Pattern.<LoginUser>begin("begin").where(new SimpleCondition<LoginUser>() {
          @Override
          public boolean filter(LoginUser value) throws Exception {
              return value.getStatus().equals("fail");
          }
      }).next("next").where(new SimpleCondition<LoginUser>() {
          @Override
          public boolean filter(LoginUser value) throws Exception {
              return value.getStatus().equals("fail");
          }
      }).within(Time.seconds(2));

      PatternStream<LoginUser> cep = CEP.pattern(source, pattern);

      cep.select(new PatternSelectFunction<LoginUser, Object>() {
          @Override
          public Object select(Map<String, List<LoginUser>> map) throws Exception {
              List<LoginUser> begin = map.get("begin");
              List<LoginUser> end = map.get("next");
              return Tuple2.of(begin, end);
          }
      }).print();

      env.execute();
  }
}
