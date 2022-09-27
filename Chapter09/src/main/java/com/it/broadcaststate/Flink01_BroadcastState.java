package com.it.broadcaststate;

import com.it.pojo.Action;
import com.it.pojo.Pattern;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author ZuYingFang
 * @time 2022-05-13 12:29
 * @description
 */
public class Flink01_BroadcastState {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 读取用户行为事件流
        DataStreamSource<Action> actionStream = env.fromElements(new Action("Alice", "login"),
                new Action("Alice", "pay"),
                new Action("Bob", "login"),
                new Action("Bob", "buy")

        );


        // 定义行为模式流，代表了要检测的标准
        DataStreamSource<Pattern> patternStream = env.fromElements(
                new Pattern("login", "pay"),
                new Pattern("login", "buy"));

        // 定义广播状态的描述器，创建广播流
        BroadcastStream<Pattern> bcPatterns = patternStream.broadcast(new MapStateDescriptor<Void, Pattern>("patterns", Types.VOID, Types.POJO(Pattern.class)));

        // 将事件流和广播流连接起来，进行处理
        SingleOutputStreamOperator<Tuple2<String, Pattern>> matches = actionStream.keyBy(data -> data.userId)
                .connect(bcPatterns)
                .process(new KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>>() {

                    // 定义一个值状态，保存上一次用户行为
                    ValueState<String> prevActionState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        prevActionState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastAction", Types.STRING));
                    }

                    @Override
                    public void processBroadcastElement(Pattern value, KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>>.Context ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {
                        BroadcastState<Void, Pattern> bcState = ctx.getBroadcastState(new MapStateDescriptor<Void, Pattern>("patterns", Types.VOID, Types.POJO(Pattern.class)));

                        // 将广播状态更新为当前的 pattern
                        bcState.put(null, value);
                    }

                    @Override
                    public void processElement(Action value, KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>>.ReadOnlyContext ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {
                        Pattern pattern = ctx.getBroadcastState(new MapStateDescriptor<>("patterns", Types.VOID, Types.POJO(Pattern.class))).get(null);

                        String prevAction = prevActionState.value();
                        if (pattern != null && prevAction != null){
                            if (pattern.action1.equals(prevAction) && pattern.action2.equals(value.action)){
                                out.collect(new Tuple2<>(ctx.getCurrentKey(),pattern));
                            }
                        }

                        // 更新状态
                        prevActionState.update(value.action);

                    }

                });

        matches.print();

        env.execute();

    }

}
