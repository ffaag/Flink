package com.it.source;

import com.it.pojo.Event;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * @author ZuYingFang
 * @time 2022-04-28 17:02
 * @description
 */
public class MyParallelSource implements ParallelSourceFunction<Event> {

    // 声明一个布尔变量，作为控制数据生成的标识位
    private Boolean running = true;

    // 使用运行时上下文对象（SourceContext）向下游发送数据；
    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        // 产生数据，在指定的数据集中随机选取数据
        Random random = new Random();
        String[] users = {"Mary", "Alice", "Bob", "Cary"};
        String[] urls = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};

        while (running) {
            ctx.collect(new Event(    // 向下游发送数据
                    users[random.nextInt(users.length)],
                    urls[random.nextInt(urls.length)],
                    Calendar.getInstance().getTimeInMillis()
            ));
            // 隔 1 秒生成一个点击事件，方便观测
            Thread.sleep(1000);
        }
    }

    // 通过标识位控制退出循环，来达到中断数据源的效果。
    @Override
    public void cancel() {
        running = false;
    }
}
