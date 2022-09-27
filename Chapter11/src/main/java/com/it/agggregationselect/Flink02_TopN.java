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
 * @time 2022-05-20 11:21
 * @description
 */
public class Flink02_TopN {

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

        // 定义子查询，进行窗口聚合，得到包含窗口信息、用户以及访问次数的结果表
        String subQuery = "SELECT window_start, window_end, user, COUNT(url) as cnt " +
                "FROM TABLE(TUMBLE(TABLE EventTable, DESCRIPTOR(ts), INTERVAL '1' HOUR))" +
                "GROUP BY window_start, window_end, user";

        // 定义 Top N 的外层查询
        String topNQuery = "SELECT * FROM " +
                "(SELECT *, ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY cnt desc) AS row_num " +
                "FROM (" + subQuery + ")) WHERE row_num <= 2";

        Table table = tableEnvironment.sqlQuery(topNQuery);

        tableEnvironment.toDataStream(table).print();

        env.execute();
    }

}
