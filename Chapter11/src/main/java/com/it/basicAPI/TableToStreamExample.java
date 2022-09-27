package com.it.basicAPI;

import com.it.pojo.Event;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author ZuYingFang
 * @time 2022-05-17 16:21
 * @description
 */
public class TableToStreamExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Event> eventStream = env.fromElements(
                new Event("Alice", "./home", 1000L),
                new Event("Bob", "./cart", 1000L),
                new Event("Alice", "./prod?id=1", 5 * 1000L),
                new Event("Cary", "./home", 60 * 1000L),
                new Event("Bob", "./prod?id=3", 90 * 1000L),
                new Event("Alice", "./prod?id=7", 105 * 1000L)
        );

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        tableEnvironment.createTemporaryView("EventTable", eventStream);

        // 流转换而来的表已经注册到位了，可以直接在sql中使用
        Table query1 = tableEnvironment.sqlQuery("select url, user from EventTable where user = 'Alice'");

        Table query2 = tableEnvironment.sqlQuery("select user, count(url) from EventTable group by user");

        // 将表转换成流进行输出
        tableEnvironment.toDataStream(query1).print();
        tableEnvironment.toChangelogStream(query2).print();

        env.execute();

    }
}
