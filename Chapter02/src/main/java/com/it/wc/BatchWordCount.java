package com.it.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author ZuYingFang
 * @time 2022-04-26 11:40
 * @description 这一套API底层使用的数据结构是DataSet，专门为批处理设置的，现在版本让批处理和流处理一起使用一套API，底层使用DataStream
 * 只是在提交任务时不一样，在提交任务时通过将执行模式设为BATCH来进行批处理： $ bin/flink run -Dexecution.runtime-mode=BATCH BatchWordCount.jar
 */
public class BatchWordCount {

    public static void main(String[] args) throws Exception {
        // 1 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2 从文件中读取数据，读取到的是一行一行的数据，这里的位置注意了，是和根项目对应的
        DataSource<String> lines = env.readTextFile("Chapter02/input/words.txt");

        // 3 转换数据格式，将其转换成二元组(hello, 1)
        // 注意了跟scala不一样，可以不写参数类型，但不可以省略，即不能写_，api完全不相同，定义一个Collector，里面放Tuple2
        // 当 Lambda 表达式使用 Java 泛型的时候, 由于泛型擦除的存在, 需要显示的声明类型信息
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOne = lines.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            for (String word : line.split(" ")) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 4 按照单词进行分组，这里注意，直接就使用元组的位置就行
        UnsortedGrouping<Tuple2<String, Long>> group = wordAndOne.groupBy(0);

        // 5 分组内进行聚合统计，一样，这里指定的是元组的位置，就是把第二个位置的值进行求和
        AggregateOperator<Tuple2<String, Long>> sum = group.sum(1);

        // 6 打印结果
        sum.print();
    }

}
