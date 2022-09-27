package com.it.transformation.aggregation;

import com.it.pojo.Event;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ZuYingFang
 * @time 2022-04-29 11:39
 * @description
 */
public class Flink02_Basic {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("a", 1), Tuple2.of("a", 3), Tuple2.of("b", 3), Tuple2.of("b", 4));

        source.keyBy(r -> r.f0).sum(1).print();  // 指定位置进行聚合
        source.keyBy(r -> r.f0).sum("f1").print();        // 指定字段名称进行聚合
        source.keyBy(r -> r.f0).max(1).print();
        source.keyBy(r -> r.f0).max("f1").print();
        source.keyBy(r -> r.f0).min(1).print();
        source.keyBy(r -> r.f0).min("f1").print();
        source.keyBy(r -> r.f0).maxBy(1).print();
        source.keyBy(r -> r.f0).maxBy("f1").print();
        source.keyBy(r -> r.f0).minBy(1).print();
        source.keyBy(r -> r.f0).minBy("f1").print();


        DataStreamSource<Event> streamSource = env.fromElements(new Event("Mary", "./home", 1000L), new Event("Bob", "./cart", 2000L));
        streamSource.keyBy(e -> e.user).max("user").print();   // pojo类指定字段名称

        env.execute();

    }

}
