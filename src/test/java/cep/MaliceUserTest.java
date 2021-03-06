package cep;

import cn.itcast.cep.bean.Message;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * 用户如果在10s内，输入了TMD 5次，就认为用户为恶意攻击，识别出该用户
 */
public class MaliceUserTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KeyedStream<Message, String> source = env.fromElements(
                new Message("1", "TMD", 1558430842000L),//2019-05-21 17:27:22
                new Message("1", "TMD", 1558430843000L),//2019-05-21 17:27:23
                new Message("1", "TMD", 1558430845000L),//2019-05-21 17:27:25
                new Message("1", "TMD", 1558430850000L),//2019-05-21 17:27:30
                new Message("1", "TMD", 1558430851000L),//2019-05-21 17:27:30
                new Message("2", "TMD", 1558430851000L),//2019-05-21 17:27:31
                new Message("1", "TMD", 1558430852000L)) //2019-05-21 17:27:32)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Message>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(Message element) {
                        return element.getEventTime();
                    }
                }).keyBy(Message::getId);

        Pattern<Message, Message> pattern = Pattern.<Message>begin("begin").where(new SimpleCondition<Message>() {
            @Override
            public boolean filter(Message value) throws Exception {
                return value.getMsg().equals("TMD");
            }
        })
                .times(4, 5)
                .within(Time.seconds(10));

        PatternStream<Message> cep = CEP.pattern(source, pattern);
        cep.select(new PatternSelectFunction<Message, Object>() {
            @Override
            public Object select(Map<String, List<Message>> map) throws Exception {
                return map.get("begin");
            }
        }).print();

        env.execute();

    }
}
