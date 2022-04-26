package cn.itcast.itcast.state;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Iterator;

/**
 * Author itcast
 * Desc 演示Flink-State-ManagedState-OperatorState
 * 使用自定义状态OperatorState- ListState来存储offset信息,模拟FlinkKafkaConsumer的offset维护
 */
public class StateDemo02 {
    public static void main(String[] args) throws Exception {
        //TODO 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //先将Checkpoint的配置拿过来用,后面学到再解释
        env.enableCheckpointing(1000);//每隔1s执行一次Checkpoint
        env.setStateBackend(new FsStateBackend("file:///D:/ckp"));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //固定延迟重启策略: 程序出现异常的时候，重启2次，每次延迟3秒钟重启，超过2次，程序退出
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 3000));

        //TODO 2.source
        DataStreamSource<String> ds = env.addSource(new MyKafkaSource()).setParallelism(1);

        //TODO 3.transformation

        //TODO 4.sink
        ds.print();

        //TODO 5.execution
        env.execute();
    }

    //使用OperatorState-ListState模拟FlinkKafkaConsumer维护offset信息
    public static class MyKafkaSource extends RichParallelSourceFunction<String> implements CheckpointedFunction{
        private boolean flag = true;
        //-1.声明Sstate用来存放offset
        private ListState<Long> offsetState = null;
        private Long offset = 0L;

        //-2.初始化状态/类似之前的open
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            //创建状态描述器
            ListStateDescriptor<Long> stateDescriptor = new ListStateDescriptor<>("offsetState", Long.class);
            //根据状态描述器初始化状态
            offsetState = context.getOperatorStateStore().getListState(stateDescriptor);
        }

        //-3.使用状态
        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            Iterator<Long> iterator = offsetState.get().iterator();
            if (iterator.hasNext()){
                offset = iterator.next();//从状态中获取存储的offset信息(我们学习时为了简单只放入了一个)
            }
            while (flag){
                offset++;
                ctx.collect("subTaskId: "+getRuntimeContext().getIndexOfThisSubtask() + " 消费到了哪个偏移量:"+ offset);
                Thread.sleep(2000);
                if(offset % 5 == 0){
                    System.out.println("发生异常了.....");
                    throw new RuntimeException("发生异常了.....");
                }
            }
        }

        @Override
        public void cancel() {
            this.flag = false;
        }

        //-4.快照状态到Checkpoint目录中
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            //System.out.println("snapshotState.....");
            offsetState.clear();//清理State并刷入Checkpoint目录中
            offsetState.add(offset);
        }
    }
}
