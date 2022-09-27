package com.it.keyedstate;

import com.it.pojo.Event;
import com.it.source.MyParallelSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @author ZuYingFang
 * @time 2022-05-12 11:52
 * @description
 */
public class Flink03_MapState {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Event> streamOperator = env.addSource(new MyParallelSource()).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));

        streamOperator.keyBy(data -> data.url)
                        .process(new KeyedProcessFunction<String, Event, String>() {

                            // 声明状态，用map来保存pv值（窗口start， count）
                            MapState<Long, Long> windowPvMapState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                windowPvMapState =	getRuntimeContext().getMapState(new MapStateDescriptor<Long, Long>("window-pv", Long.class, Long.class));
                            }

                            @Override
                            public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
                                // 每来一条数据就判断他属于哪个窗口
                                long windowStart = value.timestamp / (10 * 1000L) * (10 * 1000L);
                                long windowStop = windowStart + 10 * 1000L;

                                // 注册一个windowStop - 1 的定时器，到这个点我们就触发定时器输出状态中的值
                                ctx.timerService().registerEventTimeTimer(windowStop - 1L);

                                // 更新状态中的值
                                if (windowPvMapState.contains(windowStart)){
                                    windowPvMapState.put(windowStart, windowPvMapState.get(windowStart) + 1L);
                                }else {
                                    windowPvMapState.put(windowStart, 1L);
                                }
                            }

                            @Override
                            public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                                // 定时器触发，直接输出统计得到的pv值
                                Long windowStop = timestamp + 1;
                                Long windowStart = windowStop - 10*1000L;
                                Long pv = windowPvMapState.get(windowStart);
                                out.collect( "url: " + ctx.getCurrentKey()
                                        + " 访问量: " + pv
                                        + " 窗 口 ： " + new Timestamp(windowStart) + " ~ " + new Timestamp(windowStop)
                                );

                                // 销毁窗口，清除掉map中的key
                                windowPvMapState.remove(windowStart);
                            }
                        }).print();

        env.execute();

    }

}
