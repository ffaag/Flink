package com.it.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @author ZuYingFang
 * @time 2022-04-29 14:07
 * @description
 */
public class SaveToKafka {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "hadoop102:9092");

        DataStreamSource<String> source = env.readTextFile("./Chapter05/input/words.txt");
        source.addSink(new FlinkKafkaProducer<String>("click", // 输入的主题
                new SimpleStringSchema(),   // 序列化
                properties));

        env.execute();

    }

}
