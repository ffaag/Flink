package com.it.transformation.PhysicalPartitioning;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @author ZuYingFang
 * @time 2022-04-30 12:30
 * @description
 */
public class Flink03_Rescale {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new RichParallelSourceFunction<Integer>() {
            @Override
            public void run(SourceContext<Integer> ctx) throws Exception {
                for (int i = 1; i <= 8; i++) {
                    // 将奇偶分别发往0和1号分区
                    if (i % 2 == getRuntimeContext().getIndexOfThisSubtask())
                        ctx.collect(i);
                }
            }

            @Override
            public void cancel() {

            }
        }).setParallelism(2)
                .rescale()
                        .print().setParallelism(4);

        env.execute();

    }

}
