package com.it.keyedstate;

import com.it.pojo.Event;
import com.it.source.MyParallelSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @author ZuYingFang
 * @time 2022-05-12 11:52
 * @description
 */
public class Flink04_AggregatingState {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Event> streamOperator = env.addSource(new MyParallelSource()).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));

        streamOperator.keyBy(data -> data.user)
                .flatMap(new RichFlatMapFunction<Event, String>() {

                    // 定义聚合状态，用来计算平均时间戳
                    AggregatingState<Event, Long> avgTsAggState;
                    // 定义一个值状态，用来保存当前用户的访问频次
                    ValueState<Long> countState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        avgTsAggState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Event, Tuple2<Long, Long>, Long>("avg-ts",
                                new AggregateFunction<Event, Tuple2<Long, Long>, Long>() {
                                    @Override
                                    public Tuple2<Long, Long> createAccumulator() {
                                        return Tuple2.of(0L, 0L);
                                    }

                                    @Override
                                    public Tuple2<Long, Long> add(Event value, Tuple2<Long, Long> accumulator) {
                                        return Tuple2.of(accumulator.f0 + value.timestamp, accumulator.f1 + 1);
                                    }

                                    @Override
                                    public Long getResult(Tuple2<Long, Long> accumulator) {
                                        return accumulator.f0 / accumulator.f1;
                                    }

                                    @Override
                                    public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
                                        return null;
                                    }
                                }, Types.TUPLE(Types.LONG, Types.LONG)));

                        countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count", Long.class));
                    }

                    @Override
                    public void flatMap(Event value, Collector<String> out) throws Exception {
                        Long count = countState.value();
                        if (count == null) {
                            count = 1L;
                        }else {
                            count ++;
                        }
                        countState.update(count);
                        avgTsAggState.add(value);

                        // 达到五次就输出状态，并且清空状态
                        if (count == 5){
                            out.collect(value.user + "平均时间戳" + new Timestamp(avgTsAggState.get()));
                            countState.clear();
                        }
                    }
                }).print();

        env.execute();

    }


}
