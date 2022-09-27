package com.it.keyedstate;

import com.it.pojo.Event;
import com.it.source.MyParallelSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author ZuYingFang
 * @time 2022-05-12 11:52
 * @description
 */
public class Flink01_ValueState {

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

        // 统计每个用户的 pv，隔一段时间（10s）输出一次结果
        streamOperator.keyBy(data -> data.user)
                .process(new KeyedProcessFunction<String, Event, String>() {

                    // 定义两个状态，保存当前 pv 值和定时器时间戳
                    ValueState<Long> countState;
                    ValueState<Long> timerTsState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 从上下文环境中获取到状态，注意，处理函数默认就是富函数的
                        countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count", Long.class));
                        timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTs", Long.class));
                    }

                    @Override
                    public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
                        // 更新count的值
                        Long count = countState.value();
                        if (count == null){
                            countState.update(1L);
                        }else {
                            countState.update(count + 1L);
                        }
                        // 来一个数据就执行这个方法，我们要注册一个定时器，到时间了就触发输出数据
                        // 注意，flink中触发器是一个的，就算你来一个数据创建一个，只要当前定时器还没有被触发，那就一直是这个，不会新建
                        if(timerTsState.value() == null){
                            ctx.timerService().registerEventTimeTimer(value.timestamp + 10 * 1000L);
                        }
                        timerTsState.update(value.timestamp);
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        // 定时器处理的逻辑，即当时间满了10s触发定时器，就收集状态中的数据并且清空当前状态中的内容
                        out.collect(ctx.getCurrentKey()  + "pv: " + countState.value());
                        timerTsState.clear();
                    }

                }).print();

        env.execute();

    }
    


}
