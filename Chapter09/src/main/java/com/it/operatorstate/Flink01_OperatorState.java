package com.it.operatorstate;

import com.it.pojo.Event;
import com.it.source.MyParallelSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @author ZuYingFang
 * @time 2022-05-13 12:29
 * @description
 */
public class Flink01_OperatorState {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Event> streamOperator = env.addSource(new MyParallelSource()).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));

        streamOperator.print("input");

        // 批量缓存输出
        streamOperator.addSink(new BufferingSink(10));

        env.execute();

    }

    public static class BufferingSink implements SinkFunction<Event>, CheckpointedFunction{

        private final int threshold;
        private transient ListState<Event> checkpointedState;
        private List<Event> bufferedElements;

        public BufferingSink(int threshold) {
            this.threshold = threshold;
            this.bufferedElements = new ArrayList<>();
        }

        @Override
        public void invoke(Event value, Context context) throws Exception {
            bufferedElements.add(value);
            if (bufferedElements.size() == threshold) {
                // 输出
                for (Event bufferedElement : bufferedElements) {
                    System.out.println(bufferedElement);
                }
                System.out.println("===========输出完毕===========");
                bufferedElements.clear();
            }
        }

        // 保存状态快照到检查点时，调用这个方法
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            checkpointedState.clear();
            // 把当前局部变量中的所有元素写入检查点中
            for (Event bufferedElement : bufferedElements) {
                checkpointedState.add(bufferedElement);
            }
        }

        // 初始化状态时调用这个方法，也会在恢复状态时调用
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            context.getOperatorStateStore().getListState(new ListStateDescriptor<Event>("buffered-elements", Event.class));
            // 如果是从故障中恢复，就将 ListState 中的所有元素添加到局部变量中
            if (context.isRestored()) {
                for (Event event : checkpointedState.get()) {
                    bufferedElements.add(event);
                }
            }
        }
    }

}
