package com.it.windowassigners;

import com.it.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author ZuYingFang
 * @time 2022-05-04 17:32
 * @description
 */
public class Flink01_TimeWindow {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> streamSource = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L));

        // 这里的一系列窗口我都没有添加窗口函数的，所以只是学习使用

        // 滚动窗口事件时间
        streamSource.keyBy(data -> data.user).window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)));

        // 滚动窗口处理时间
        streamSource.keyBy(data -> data.user).window(TumblingProcessingTimeWindows.of(Time.days(1)));


        // 滑动窗口事件时间
        streamSource.keyBy(data -> data.user).window(SlidingEventTimeWindows.of(Time.days(1), Time.hours(12)));

        // 滑动窗口处理时间
        streamSource.keyBy(data -> data.user).window(SlidingProcessingTimeWindows.of(Time.days(1), Time.hours(12)));


        // 会话窗口事件时间
        streamSource.keyBy(data -> data.user).window(EventTimeSessionWindows.withGap(Time.days(1)));

        // 会话窗口处理时间
        streamSource.keyBy(data -> data.user).window(ProcessingTimeSessionWindows.withGap(Time.days(1)));


//        这里.withDynamicGap()方法需要传入一个 SessionWindowTimeGapExtractor 作为参数，
//        用来定义 session gap 的动态提取逻辑。在这里，我们提取了数据元素的第一个字段，
//        用它的长度乘以 1000 作为会话超时的间隔。
//        streamSource.keyBy(data -> data.user).window(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<Tuple2<String, Long>>() {
//
//            @Override
//            public long extract(Tuple2<String, Long> element) {
//                return element.f0.length() * 1000;
//            }
//        }));
    }

}
