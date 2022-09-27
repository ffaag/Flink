package com.it.processfunction;

import com.it.pojo.Event;
import com.it.source.MyParallelSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @author ZuYingFang
 * @time 2022-05-06 12:39
 * @description
 */
public class Flink02_KeyedProcessFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 使用处理时间，因此不需要设置水位线以及提取时间戳
        SingleOutputStreamOperator<Event> streamOperator = env.addSource(new MyParallelSource());


        // <K> – Type of the key.
        // <I> – Type of the input elements.
        // <O> – Type of the output elements.
        streamOperator.keyBy(data -> true).process(new KeyedProcessFunction<Boolean, Event, String>() {
            @Override
            public void processElement(Event value, KeyedProcessFunction<Boolean, Event, String>.Context ctx, Collector<String> out) throws Exception {
                long currTs = ctx.timerService().currentProcessingTime(); // 获取到当前处理时间
                out.collect("数据到达，到达时间：" + new Timestamp(currTs));

                // 注册一个10秒后的定时器，注册了定时器那就要定义定时器的逻辑onTimer()
                ctx.timerService().registerProcessingTimeTimer(currTs + 10 * 1000L);
            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<Boolean, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                out.collect("定时器触发，触发时间为：" + new Timestamp(timestamp));
            }
        }).print();

        env.execute();
    }

}
