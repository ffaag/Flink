package com.it.windowassigners;

import com.it.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ZuYingFang
 * @time 2022-05-04 17:32
 * @description
 */
public class Flink02_CountWindow {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> streamSource = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L));

        // 这里的一系列窗口我都没有添加窗口函数的，所以只是学习使用

        // 滚动计数窗口
        streamSource.keyBy(data -> data.user).countWindow(10);

        // 滑动计数窗口
        streamSource.keyBy(data -> data.user).countWindow(10, 3);
    }

}
