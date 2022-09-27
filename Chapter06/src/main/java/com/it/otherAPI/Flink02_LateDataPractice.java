package com.it.otherAPI;

import com.it.pojo.Event;
import com.it.pojo.UrlViewCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author ZuYingFang
 * @time 2022-05-05 11:53
 * @description
 */
public class Flink02_LateDataPractice {

    // 算出10s内的url访问量，5s更新一次
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Event> streamOperator = env.socketTextStream("localhost", 7777).map(data -> {
            String[] split = data.split(" ");
            return new Event(split[0].trim(), split[1].trim(), Long.valueOf(split[2].trim()));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2)) // 方式一，设置水位线延迟时间
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));

        // 定义侧输出流标签
        OutputTag<Event> outputTag = new OutputTag<Event>("late") {
        };

        SingleOutputStreamOperator<UrlViewCount> result = streamOperator.keyBy(data -> data.url).window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.minutes(1))   // 方式二：允许窗口处理迟到数据，设置 1 分钟的等待时间
                .sideOutputLateData(outputTag)      // 方式三：将最后的迟到数据输出到侧输出流
                .aggregate(new AggregateFunction<Event, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(Event value, Long accumulator) {
                        return accumulator + 1L;
                    }

                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        return null;
                    }
                }, new ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow>() {
                    @Override  // 这里的UrlViewCount是我们自定义的pojo类
                    public void process(String url, ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow>.Context context, Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        out.collect(new UrlViewCount(url, elements.iterator().next(), start, end));
                    }
                });

        result.print("result");
        // 侧输出流需要手动输出
        result.getSideOutput(outputTag).print("late");

        // 为方便观察，可以将原始数据也输出
        streamOperator.print("input");

        env.execute();

    }
}
