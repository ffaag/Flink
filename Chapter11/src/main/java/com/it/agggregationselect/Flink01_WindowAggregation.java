package com.it.agggregationselect;

import com.it.pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author ZuYingFang
 * @time 2022-05-19 11:12
 * @description
 */
public class Flink01_WindowAggregation {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Event> eventStream = env.fromElements(
                new Event("Alice", "./home", 1000L),
                new Event("Bob", "./cart", 1000L),
                new Event("Alice", "./prod?id=1", 25 * 60 * 1000L),
                new Event("Alice", "./prod?id=4", 55 * 60 * 1000L),
                new Event("Bob", "./prod?id=5", 3600 * 1000L + 60 * 1000L),
                new Event("Cary", "./home", 3600 * 1000L + 30 * 60 * 1000L),
                new Event("Cary", "./prod?id=7", 3600 * 1000L + 59 * 60 * 1000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps().
                withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        tableEnvironment.createTemporaryView("EventTable", eventStream, $("user"), $("url"), $("timestamp").rowtime().as("ts"));

        Table table = tableEnvironment.sqlQuery("SELECT user, window_end as endT, count(url) as cnt from " +
                "TABLE(CUMULATE(TABLE EventTable, DESCRIPTOR(ts), INTERVAL '30' MINUTE, INTERVAL '1' HOUR)) " +
                "GROUP BY user, window_start, window_end");

        tableEnvironment.toDataStream(table).print();   // 用的是时间窗口，所以可以直接用toDataStream

        env.execute();
    }
}
