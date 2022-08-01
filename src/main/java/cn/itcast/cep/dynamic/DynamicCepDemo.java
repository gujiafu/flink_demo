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

import javax.script.ScriptException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class DynamicCepDemo {

  public static void main(String[] args) throws Exception {

      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      // 数据指标来源
      DataStreamSource<MetricEvent> metricEvent = env.addSource(new MetricEventSourceFunction());
      // cep规则来源
      Pattern pattern = getPattern();

      PatternStream pStream2 = CEP.pattern(metricEvent, pattern).inProcessingTime();
      SingleOutputStreamOperator filter2 = pStream2.select(new PatternSelectFunction<MetricEvent, String>() {
          @Override
          public String select(Map<String, List<MetricEvent>> pattern) throws Exception {
              return "-----------------------------" + pattern.toString();
          }
      });

      filter2.print();

      env.execute("----flink cep alert ----");
  }


  static Pattern getPattern() throws Exception {
      Pattern p1 = ScriptEngine.getPattern(

              "  import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy\n" +
                      "import org.apache.flink.cep.pattern.Pattern\n" +
                      "import cn.itcast.cep.dynamic.AviatorCondition \n" +

                      "where1 = new AviatorCondition(" +
                      "   \"getT(tags,\\\"cluster_name\\\")==\\\"terminus-x3\\\"&&getF(fields,\\\"load5\\\")==3 \"" +
                      "        )\n" +

                      "def get(){ " +
                      "      return Pattern.begin(\"start\")\n" +
                      "        .where(where1)" +
                      "}",
              "get");

      Pattern p2 = ScriptEngine.getPattern(

              "  import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy\n" +
                      "import org.apache.flink.cep.pattern.Pattern\n" +
                      "import cn.itcast.cep.dynamic.AviatorCondition \n" +

                      "where1 = new AviatorCondition(" +
                      "   \"getT(tags,\\\"cluster_name\\\")==\\\"terminus-x2\\\"&&getF(fields,\\\"load5\\\")==2 \"" +
                      "        )\n" +

                      "def get(){ " +
                      "      return Pattern.begin(\"start\")\n" +
                      "        .where(where1)" +
                      "}",
              "get");
      return p2;
  }

  static class MetricEventSourceFunction implements SourceFunction<MetricEvent>{

      @Override
      public void run(SourceContext<MetricEvent> ctx) throws Exception {
          Random random = new Random();

          List<String> nameList = Lists.newArrayList("zzz", "aaa", "bbb");

          Map<String, Object> fieldsMap1 = new HashMap<>();
          fieldsMap1.put("1", 1d);
          fieldsMap1.put("10", 10d);
          fieldsMap1.put("100", 100d);
          fieldsMap1.put("load5", 1d);
          Map<String, Object> fieldsMap2 = new HashMap<>();
          fieldsMap2.put("2", 2d);
          fieldsMap2.put("20", 20d);
          fieldsMap2.put("200", 200d);
          fieldsMap2.put("load5", 2d);
          Map<String, Object> fieldsMap3 = new HashMap<>();
          fieldsMap3.put("3", 3d);
          fieldsMap3.put("30", 30d);
          fieldsMap3.put("300", 300d);
          fieldsMap3.put("load5", 3d);
          List<Map<String, Object>> fieldsMapList = Lists.newArrayList(fieldsMap1, fieldsMap2, fieldsMap3);

          Map<String, String> tagsMap1 = new HashMap<>();
          tagsMap1.put("1", "1");
          tagsMap1.put("10", "10");
          tagsMap1.put("100", "100");
          tagsMap1.put("cluster_name", "terminus-x1");
          Map<String, String> tagsMap2 = new HashMap<>();
          tagsMap2.put("2", "2");
          tagsMap2.put("20", "20");
          tagsMap2.put("200", "200");
          tagsMap2.put("cluster_name", "terminus-x2");
          Map<String, String> tagsMap3 = new HashMap<>();
          tagsMap3.put("3", "3");
          tagsMap3.put("30", "30");
          tagsMap3.put("300", "300");
          tagsMap3.put("cluster_name", "terminus-x3");
          List<Map<String, String>> tagsMapList = Lists.newArrayList(tagsMap1, tagsMap2, tagsMap3);


          while (true){
              MetricEvent metricEvent = new MetricEvent();
              metricEvent.setName(nameList.get(random.nextInt(3)));
              metricEvent.setTimestamp(System.currentTimeMillis());
              metricEvent.setFields(fieldsMapList.get(random.nextInt(3)));
              metricEvent.setTags(tagsMapList.get(random.nextInt(3)));
              ctx.collect(metricEvent);
              Thread.sleep(1000);
          }
      }

      @Override
      public void cancel() {

      }
  }
}
