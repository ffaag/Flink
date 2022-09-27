package com.it.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author ZuYingFang
 * @time 2022-04-26 12:17
 * @description
 */
public class BoundedStreamWordCount {

    public static void main(String[] args) throws Exception {
        // 1 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2 从文件中读取数据，读取到的是一行一行的数据，这里的位置注意了，是和根项目对应的
        DataStreamSource<String> lines = env.readTextFile("Chapter02/input/words.txt");

        // 3 转换数据格式，将其转换成二元组(hello, 1)
        // 注意了跟scala不一样，可以不写参数类型，但不可以省略，即不能写_，api完全不相同，定义一个Collector，里面放Tuple2
        // 当 Lambda 表达式使用 Java 泛型的时候, 由于泛型擦除的存在, 需要显示的声明类型信息
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = lines.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            for (String word : line.split(" ")) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 流处理就不用groupBy了，用keyBy，里面放一个函数，这就跟Scala有点像，f0是元组的位置
        KeyedStream<Tuple2<String, Long>, String> keyedStream = wordAndOne.keyBy(data -> data.f0);

        // 5 分组内进行聚合统计，一样，这里指定的是元组的位置，就是把第二个位置的值进行求和
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = keyedStream.sum(1);

        // 6 打印结果
        sum.print();

        // 7 执行
        env.execute();
    }


}
