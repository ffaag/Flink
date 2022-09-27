package com.it.windowfunction;

import com.it.pojo.Event;
import com.it.source.MyParallelSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

/**
 * @author ZuYingFang
 * @time 2022-05-05 11:53
 * @description
 */
public class Flink03_ProcessWindowFunction {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Event> streamOperator = env.addSource(new MyParallelSource()).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));

        streamOperator.keyBy(data -> true).window(TumblingEventTimeWindows.of(Time.seconds(5)))
                // <IN> – The type of the input value.
                // <OUT> – The type of the output value.
                // <KEY> – The type of the key.
                // <W> – The type of Window that this window
                .process(new ProcessWindowFunction<Event, String, Boolean, TimeWindow>() {
                    @Override
                    public void process(Boolean aBoolean, ProcessWindowFunction<Event, String, Boolean, TimeWindow>.Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
                        HashSet<String> userSet = new HashSet<>();
                        // 遍历所有数据，放到 Set 里去重
                        for (Event event : elements) {
                            userSet.add(event.user);
                        }
                        // 结合窗口信息，包装输出内容
                        Long start = context.window().getStart();
                        Long end = context.window().getEnd();
                        out.collect(" 窗 口 : " + new Timestamp(start) + " ~ " + new Timestamp(end) + " 的独立访客数量是：" + userSet.size());
                    }
                }).print();


        env.execute();

    }
}
