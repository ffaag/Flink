package com.it.watermark;


import com.it.pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ZuYingFang
 * @time 2022-05-01 12:18
 * @description
 */
public class Flink01_OrderedStream {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(100);    // 手动设置水位线周期，默认为200ms，单位为毫秒

        DataStreamSource<Event> streamSource = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L));

        // 设置水位线，有序列直接把数据的时间戳作为水位线即可
        SingleOutputStreamOperator<Event> stream = streamSource.assignTimestampsAndWatermarks(WatermarkStrategy
                .<Event>forMonotonousTimestamps()  // 水位线生成器
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override   // 将数据中的 timestamp 字段提取出来， 作为时间戳分配给数据元素；
                    public long extractTimestamp(Event element, long recordTimestamp) { // 时间戳分配器

                        return element.timestamp;
                    }
                }));
    }

}
