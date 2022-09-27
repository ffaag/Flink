package com.it.transformation;

import com.it.pojo.Event;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ZuYingFang
 * @time 2022-04-30 15:08
 * @description
 */
public class UDF_RichFunction {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> eventDataStreamSource = env.fromElements(new Event("Mary", "./home", 1000L), new Event("Bob", "./cart", 2000L));

        SingleOutputStreamOperator<Integer> map = eventDataStreamSource.map(new RichMapFunction<Event, Integer>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println("open生命周期被调用" + getRuntimeContext().getIndexOfThisSubtask() + "号开始");
            }

            @Override
            public void close() throws Exception {
                super.close();
                System.out.println("close生命周期被调用" + getRuntimeContext().getIndexOfThisSubtask() + "号结束");
            }

            @Override
            public Integer map(Event event) throws Exception {
                return event.url.length();
            }
        });

        map.print();

        env.execute();
    }

}
