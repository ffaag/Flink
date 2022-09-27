package com.it.transformation.basic;

import com.it.pojo.Event;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author ZuYingFang
 * @time 2022-04-29 11:39
 * @description
 */
public class Flink03_FlatMap {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> streamSource = env.fromElements(new Event("Mary", "./home", 1000L), new Event("Bob", "./cart", 2000L));

        SingleOutputStreamOperator<String> streamOperator = streamSource.flatMap((Event event, Collector<String> out) -> {
            if (event.user.equals("Mary")) {
                out.collect(event.user);
            } else if (event.user.equals("Bob")) {
                out.collect(event.user);
                out.collect(event.url);
            }
        }).returns(Types.STRING);  // 这里用到了集合，因此会出现泛型擦除，必须指定返回类型

        streamOperator.print();

        env.execute();

    }

}
