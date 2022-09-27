package com.it.transformation.PhysicalPartitioning;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ZuYingFang
 * @time 2022-04-30 12:30
 * @description
 */
public class Flink05_Custom {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        // 使用匿名类
        DataStream<Integer> integerDataStream = source.partitionCustom(new Partitioner<Integer>() {
            @Override
            public int partition(Integer integer, int numPartitions) {
                return integer % 2;
            }  // 分区规则
        }, new KeySelector<Integer, Integer>() {
            @Override
            public Integer getKey(Integer integer) throws Exception {
                return integer;   // 选择哪个作为key来分区
            }
        });

        // 使用lambda方式
        DataStream<Integer> integerDataStream1 = source.partitionCustom((Integer integer, int i) -> {
            return integer % 2;
        }, (Integer integer) -> {
            return integer;
        });

        //integerDataStream.print();

        integerDataStream1.print();

        env.execute();

    }

}
