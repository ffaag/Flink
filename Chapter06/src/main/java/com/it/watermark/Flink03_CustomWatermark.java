package com.it.watermark;

import com.it.pojo.Event;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ZuYingFang
 * @time 2022-05-04 16:04
 * @description
 */
public class Flink03_CustomWatermark {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(100);    // 手动设置水位线周期，默认为200ms，单位为毫秒

        DataStreamSource<Event> streamSource = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L));

        streamSource.assignTimestampsAndWatermarks(new WatermarkStrategy<Event>() {
            @Override
            public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new WatermarkGenerator<Event>() {

                    private Long delayTime = 5000L;    // 延迟时间，单位ms

                    private Long maxTime = Long.MIN_VALUE + delayTime + 1L;  // 观察到的最大时间戳

                    @Override
                    public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
                        // 每来一条数据就更新一次
                        maxTime = Math.max(event.timestamp, maxTime);
                    }

                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {
                        // 发射水位线，默认 200ms 调用一次
                        output.emitWatermark(new Watermark(maxTime - delayTime - 1L));
                    }
                };
            }

        }.withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event element, long recordTimestamp) {
                return element.timestamp;
            }
        }));


        streamSource.assignTimestampsAndWatermarks(new CustomWaterStrategy());

    }

    public static class CustomWaterStrategy implements WatermarkStrategy<Event> {

        @Override
        public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new CustomPeriodicGenerator();
        }

        @Override
        public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new SerializableTimestampAssigner<Event>() {
                @Override
                public long extractTimestamp(Event element, long recordTimestamp) {
                    return element.timestamp;
                }
            };
        }
    }

    // 周期性水位线生成器（Periodic Generator）
    public static class CustomPeriodicGenerator implements WatermarkGenerator<Event> {

        private Long delayTime = 5000L;    // 延迟时间，单位ms

        private Long maxTime = Long.MIN_VALUE + delayTime + 1L;  // 观察到的最大时间戳

        @Override
        public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {

            // 每来一条数据就更新一次
            maxTime = Math.max(event.timestamp, maxTime);

        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {

            // 发射水位线，默认 200ms 调用一次
            output.emitWatermark(new Watermark(maxTime - delayTime - 1L));

        }
    }

    // 断点式水位线生成器（Punctuated Generator）
    public static class CustomPunctuatedGenerator implements WatermarkGenerator<Event> {
        @Override
        public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {

            // 只有在遇到特定的 itemId 时，才发出水位线
            if (event.user.equals("Mary")) {
                output.emitWatermark(new Watermark(event.timestamp - 1));
            }

        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // 不需要做任何事情，因为我们在 onEvent 方法中发射了水位线
        }
    }

}