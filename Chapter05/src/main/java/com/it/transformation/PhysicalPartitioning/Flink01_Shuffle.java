package com.it.transformation.PhysicalPartitioning;

import com.it.pojo.Event;
import com.it.source.custom.MySource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ZuYingFang
 * @time 2022-04-30 12:30
 * @description
 */
public class Flink01_Shuffle {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> streamSource = env.addSource(new MySource());

        // 洗牌
        DataStream<Event> shuffle = streamSource.shuffle();

        shuffle.print("shuffle");

        env.execute();

    }

}
