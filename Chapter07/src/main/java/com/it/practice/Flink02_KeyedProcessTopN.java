package com.it.practice;

import com.it.pojo.Event;
import com.it.pojo.UrlViewCount;
import com.it.source.MyParallelSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * @author ZuYingFang
 * @time 2022-05-06 12:39
 * @description
 */
public class Flink02_KeyedProcessTopN {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Event> streamOperator = env.addSource(new MyParallelSource()).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));

        // 按照url分组，求出每个url的访问量，并且把每个窗口的内容包装成一个UrlViewCount
        SingleOutputStreamOperator<UrlViewCount> urlViewCountStream = streamOperator.keyBy(data -> data.url).window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new AggregateFunction<Event, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(Event value, Long accumulator) {
                        return accumulator + 1L;
                    }

                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        return null;
                    }
                }, new ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow>() {
                    @Override  // 一个窗口接收完毕做下面的事
                    public void process(String url, ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow>.Context context, Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {
                        // 结合窗口信息，包装输出内容
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        out.collect(new UrlViewCount(url, elements.iterator().next(), start, end));
                    }
                });

        // 对结果中同一个窗口的统计数据进行排序处理
        SingleOutputStreamOperator<String> result = urlViewCountStream.keyBy(data -> data.windowEnd).process(new TopN(2));

        result.print("result: ");

        env.execute();
    }

    public static class TopN extends KeyedProcessFunction<Long, UrlViewCount, String> {

        // TopN中的n作为属性传入
        private Integer n;
        // 定义一个列表状态来存储UrlViewCount
        private ListState<UrlViewCount> urlViewCountListState;

        public TopN(Integer n) {
            this.n = n;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            urlViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<UrlViewCount>("url-view-count-list", Types.POJO(UrlViewCount.class)));
        }

        @Override
        public void processElement(UrlViewCount value, KeyedProcessFunction<Long, UrlViewCount, String>.Context ctx, Collector<String> out) throws Exception {
            // 将新来的UrlViewCount保存到状态中
            urlViewCountListState.add(value);
            // 注册一个window.end+1ms的定时器，即这个窗口的数据都到了我们就开始排序
            ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey() + 1);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, UrlViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            // 将数据从状态中取出，放到list中，方便排序
            ArrayList<UrlViewCount> urlViewCountArrayList = new ArrayList<>();
            for (UrlViewCount urlViewCount : urlViewCountListState.get()) {
                urlViewCountArrayList.add(urlViewCount);
            }
            // 拿到了状态里面的内容后就将其清空
            urlViewCountListState.clear();

            // 排序
            urlViewCountArrayList.sort(new Comparator<UrlViewCount>() {
                @Override
                public int compare(UrlViewCount o1, UrlViewCount o2) {
                    return o2.count.intValue() - o1.count.intValue();
                }
            });

            // 取前两名
            StringBuilder result = new StringBuilder();
            result.append("=============================\n");
            result.append("窗口结束时间： " + new Timestamp(timestamp - 1) + "\n");
            for (int i = 0; i < this.n; i++) {
                UrlViewCount urlViewCount = urlViewCountArrayList.get(i);
                String info = "浏览量 No." + (i + 1) +
                        "  url：" + urlViewCount.url +
                        "  浏览量：" + urlViewCount.count + "\n";
                result.append(info);
            }
            result.append("=============================\n");
            out.collect(result.toString());
        }
    }

}
