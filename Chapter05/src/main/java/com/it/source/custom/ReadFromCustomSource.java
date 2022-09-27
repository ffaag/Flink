package com.it.source.custom;

import com.it.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ZuYingFang
 * @time 2022-04-28 17:09
 * @description
 */
public class ReadFromCustomSource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> eventDataStreamSource = env.addSource(new MyParallelSource()).setParallelism(2);

        eventDataStreamSource.print();

        env.execute();

    }

}
