package com.it.transformation.basic;

import com.it.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ZuYingFang
 * @time 2022-04-29 11:39
 * @description
 */
public class Flink02_Filter {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> streamSource = env.fromElements(new Event("Mary", "./home", 1000L), new Event("Bob", "./cart", 2000L));

        // 注意，里面要得到的是bool结果，和之前一样，三种方式实现，我这里就只用lambda了，因为这个方便多了
        SingleOutputStreamOperator<Event> filter = streamSource.filter((Event event) -> {
            return "Mary".equals(event.user);
        });

        filter.print();

        env.execute();

    }

}
