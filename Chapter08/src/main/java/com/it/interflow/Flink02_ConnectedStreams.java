package com.it.interflow;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @author ZuYingFang
 * @time 2022-05-09 12:09
 * @description
 */
public class Flink02_ConnectedStreams {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> streamSource1 = env.fromElements(1, 2, 3);

        DataStreamSource<Long> streamSource2 = env.fromElements(1L, 2L, 3L);

        ConnectedStreams<Integer, Long> connectedStreams = streamSource1.connect(streamSource2);
        connectedStreams.map(new CoMapFunction<Integer, Long, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return "Integer: " + value;
            }

            @Override
            public String map2(Long value) throws Exception {
                return "Long: " + value;
            }
        }).print();

        env.execute();

    }

}
