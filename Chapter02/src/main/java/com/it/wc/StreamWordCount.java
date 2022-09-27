package com.it.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author ZuYingFang
 * @time 2022-04-26 12:53
 * @description
 */
public class StreamWordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 直接从main方法的参数中获取主机ip和参数,运行程序之前先Edit Configurations  设置好parameter参数--host 127.0.0.1 --port 7777
//        ParameterTool parameterTool = ParameterTool.fromArgs(args);
//        String hostName = parameterTool.get("host");
//        Integer port = parameterTool.getInt("port");
//        DataStreamSource<String> textStream = env.socketTextStream(hostName, port);

        DataStreamSource<String> textStream = env.socketTextStream("hadoop102", 7777);

        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = textStream.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            for (String word : line.split(" ")) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));


        KeyedStream<Tuple2<String, Long>, String> keyedStream = wordAndOne.keyBy(data -> data.f0);

        SingleOutputStreamOperator<Tuple2<String, Long>> sum = keyedStream.sum(1);

        sum.print();

        env.execute();
    }


}
