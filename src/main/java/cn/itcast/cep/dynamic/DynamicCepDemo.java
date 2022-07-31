package cn.itcast.cep.dynamic;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class DynamicCepDemo {

  public static void main(String[] args) throws Exception {

      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      DataStreamSource<MetricEvent> metricEvent = env.addSource(new MetricEventSourceFunction());

      Pattern p1 = ScriptEngine.getPattern(

              "  import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy\n" +
                      "import org.apache.flink.cep.pattern.Pattern\n" +
                      "import cn.itcast.cep.dynamic.AviatorCondition \n" +

                      "where1 = new AviatorCondition(" +
                      "   \"getT(tags,\\\"cluster_name\\\")==\\\"terminus-x\\\"&&getF(fields,\\\"load5\\\")>15 \"" +
                      "        )\n" +

                      "def get(){ " +
                      "      return Pattern.begin(\"start\", AfterMatchSkipStrategy.noSkip())\n" +
                      "        .where(where1)" +
                      "}",
              "get");

      PatternStream pStream2 = CEP.pattern(metricEvent.keyBy(metricEvent1 -> metricEvent1.getName() + Joiner.on(",").join(metricEvent1.getTags().values())), p1).inProcessingTime();
      SingleOutputStreamOperator filter2 = pStream2.select(new PatternSelectFunction<MetricEvent, String>() {
          @Override
          public String select(Map<String, List<MetricEvent>> pattern) throws Exception {
              return "-----------------------------" + pattern.toString();
          }
      });

      filter2.print();

      env.execute("----flink cep alert ----");
  }


  static class MetricEventSourceFunction implements SourceFunction<MetricEvent>{

      @Override
      public void run(SourceContext<MetricEvent> ctx) throws Exception {
          Random random = new Random();

          List<String> nameList = Lists.newArrayList("zzz", "aaa", "bbb");

          Map<String, Object> fieldsMap1 = new HashMap<>();
          fieldsMap1.put("1", 1);
          fieldsMap1.put("10", 10);
          fieldsMap1.put("100", 100);
          Map<String, Object> fieldsMap2 = new HashMap<>();
          fieldsMap1.put("2", 2);
          fieldsMap1.put("20", 20);
          fieldsMap1.put("200", 200);
          Map<String, Object> fieldsMap3 = new HashMap<>();
          fieldsMap1.put("3", 3);
          fieldsMap1.put("30", 30);
          fieldsMap1.put("300", 300);
          List<Map<String, Object>> fieldsMapList = Lists.newArrayList(fieldsMap1, fieldsMap2, fieldsMap3);

          Map<String, String> tagsMap1 = new HashMap<>();
          fieldsMap1.put("1", "1");
          fieldsMap1.put("10", "10");
          fieldsMap1.put("100", "100");
          Map<String, String> tagsMap2 = new HashMap<>();
          fieldsMap1.put("2", "2");
          fieldsMap1.put("20", "20");
          fieldsMap1.put("200", "200");
          Map<String, String> tagsMap3 = new HashMap<>();
          fieldsMap1.put("3", "3");
          fieldsMap1.put("30", "30");
          fieldsMap1.put("300", "300");
          List<Map<String, String>> tagsMapList = Lists.newArrayList(tagsMap1, tagsMap2, tagsMap3);


          while (true){
              MetricEvent metricEvent = new MetricEvent();
              metricEvent.setName(nameList.get(random.nextInt(2)));
              metricEvent.setTimestamp(System.currentTimeMillis());
              metricEvent.setFields(fieldsMapList.get(random.nextInt(2)));
              metricEvent.setTags(tagsMapList.get(random.nextInt(2)));
              ctx.collect(metricEvent);
              Thread.sleep(1000);
          }
      }

      @Override
      public void cancel() {

      }
  }
}
