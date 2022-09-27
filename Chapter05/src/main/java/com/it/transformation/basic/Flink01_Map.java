package com.it.transformation.basic;

import com.it.pojo.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ZuYingFang
 * @time 2022-04-29 11:39
 * @description
 */
public class Flink01_Map {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> streamSource = env.fromElements(new Event("Mary", "./home", 1000L), new Event("Bob", "./cart", 2000L));

        // 方法需要传入的参数是接口 MapFunction 的实现；三种方法
        // 1 要么直接写lambda函数，简单有效快捷
        SingleOutputStreamOperator<String> map = streamSource.map((Event e) -> {
            return e.user;
        });

        // 2 要么 传入匿名类，实现 MapFunction，两个泛型，分别是输入类型和输出类型
        streamSource.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event event) throws Exception {
                return event.user;
            }
        });

        // 3 要么传入 MapFunction 的实现类，那就是单独在外面实现MapFunction，然后在这里调用 streamSource.map(new UserExtractor());

        map.print();

        env.execute();

    }

}
