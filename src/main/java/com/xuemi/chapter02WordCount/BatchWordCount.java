package com.xuemi.chapter02WordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchWordCount {

    public static void main(String[] args) throws Exception{
        //1. 创建一个批处理执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2. 读取文本文件
        DataSource<String> lindDS = env.readTextFile("input/chapter02WordCount.txt");

        //3. 单词切割后组成元祖(word, 1)
        FlatMapFunction<String, Tuple2<String, Long>> flatMapFunction = (line, out)->{
            String[] words = line.split(" ");//按照空格切割单词
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        };
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOne = lindDS.flatMap(flatMapFunction).returns(Types.TUPLE(Types.STRING, Types.LONG));
//        FlatMapOperator<String, Tuple2<String, Long>> wordAndOne = lindDS.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
//            String[] words = line.split(" ");//按照空格切割单词
//            for (String word : words) {
//                out.collect(Tuple2.of(word, 1L));
//            }
//        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        //4. 分组：按照Tuple元祖的第一个元素进行分组
        UnsortedGrouping<Tuple2<String, Long>> wordAndOneUG = wordAndOne.groupBy(t -> t.f0);

        //5. 求和：按照Tuple的第二个元素进行求和
        AggregateOperator<Tuple2<String, Long>> sum = wordAndOneUG.sum(1);

        //6. 打印结果
        sum.print();
    }

}
