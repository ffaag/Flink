package com.it.windowfunction;

import com.it.pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

/**
 * @author ZuYingFang
 * @time 2022-05-05 11:53
 * @description
 */
public class Flink01_ReduceFunction {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> streamSource = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "./prod?id=2", 2500L),
                new Event("Alice", "./prod?id=300", 3600L),
                new Event("Bob", "./home", 3000L),
                new Event("Bob", "./prod?id=1", 2300L),
                new Event("Bob", "./prod?id=3", 3300L));

        // 设置水位线，在真实操作中直接接在数据源后面，不需要另外起行，我这里是为了演示的更加清楚
        SingleOutputStreamOperator<Event> streamOperator = streamSource.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));


        // 下面的都可以搞在一起，为了演示所以我分开了，搞在一起都不用returns了
        SingleOutputStreamOperator<Tuple2<String, Integer>> returns = streamOperator.map(data -> Tuple2.of(data.user, 1)).returns(Types.TUPLE(Types.STRING, Types.INT));

         // 滚动时间时间窗口
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> window = returns.keyBy(data -> data.f0).window(TumblingEventTimeWindows.of(Time.seconds(5)));

        // 归约聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> returns1 = window.reduce((data1, data2) -> {
            return Tuple2.of(data1.f0, data1.f1 + data2.f1);
        }).returns(Types.TUPLE(Types.STRING, Types.INT));

        returns1.print();

        env.execute();

    }
}
