package com.it.interflow;

import com.it.pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author ZuYingFang
 * @time 2022-05-09 11:46
 * @description
 */
public class Flink01_Union {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Event> streamSource1 = env.socketTextStream("localhost", 8888).map(data -> {
            String[] split = data.split(",");
            return new Event(split[0], split[1], Long.valueOf(split[2]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));

        SingleOutputStreamOperator<Event> streamSource2 = env.socketTextStream("localhost", 7777).map(data -> {
            String[] split = data.split(",");
            return new Event(split[0], split[1], Long.valueOf(split[2]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event element, long recordTimestamp) {
                return element.timestamp;
            }
        }));

        streamSource1.union(streamSource2).process(new ProcessFunction<Event, String>() {
            @Override
            public void processElement(Event value, ProcessFunction<Event, String>.Context ctx, Collector<String> out) throws Exception {
                out.collect("水位线：" + ctx.timerService().currentWatermark());
            }
        }).print();

        env.execute();

    }

}
