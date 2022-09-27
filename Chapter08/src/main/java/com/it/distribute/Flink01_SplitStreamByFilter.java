package com.it.distribute;

import com.it.pojo.Event;
import com.it.source.MyParallelSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ZuYingFang
 * @time 2022-05-09 11:34
 * @description
 */
public class Flink01_SplitStreamByFilter {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> streamSource = env.addSource(new MyParallelSource());

        SingleOutputStreamOperator<Event> maryStream = streamSource.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                return value.user.equals("Mary");
            }
        });

        SingleOutputStreamOperator<Event> bobStream = streamSource.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                return value.user.equals("Bob");
            }
        });

        SingleOutputStreamOperator<Event> otherStream = streamSource.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                return !value.user.equals("Mary") && !value.user.equals("Bob");
            }
        });

        maryStream.print("maryStream");
        bobStream.print("bobStream");
        otherStream.print("otherStream");

        env.execute();

    }
}
