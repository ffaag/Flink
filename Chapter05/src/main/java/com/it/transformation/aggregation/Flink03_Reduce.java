package com.it.transformation.aggregation;

import com.it.pojo.Event;
import com.it.source.custom.MySource;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ZuYingFang
 * @time 2022-04-29 11:39
 * @description
 */
public class Flink03_Reduce {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> streamSource = env.addSource(new MySource());

        SingleOutputStreamOperator<Tuple2<String, Integer>> map = streamSource.map(event -> {
            return Tuple2.of(event.user, 1);
        }).returns(Types.TUPLE(Types.STRING, Types.INT));

        KeyedStream<Tuple2<String, Integer>, String> tuple2StringKeyedStream = map.keyBy(tuple -> tuple.f0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = tuple2StringKeyedStream.reduce((tuple1, tuple2) -> {
            return Tuple2.of(tuple1.f0, tuple1.f1 + tuple2.f1);
        }).returns(Types.TUPLE(Types.STRING, Types.INT));

        KeyedStream<Tuple2<String, Integer>, Boolean> tuple2BooleanKeyedStream = reduce.keyBy(tuple -> true);// 为每一条数据分配同一个 key，将聚合结果发送到一条流中去

        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce1 = tuple2BooleanKeyedStream.reduce((tuple1, tuple2) -> {
            return tuple1.f1 > tuple2.f1 ? tuple1 : tuple2;
        }).returns(Types.TUPLE(Types.STRING, Types.INT));

        reduce1.print();

        env.execute();

    }

}
