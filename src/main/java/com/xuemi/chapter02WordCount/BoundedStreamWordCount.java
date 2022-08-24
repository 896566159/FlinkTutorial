package com.xuemi.chapter02WordCount;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class BoundedStreamWordCount {

    public static void main(String[] args) throws Exception{
        //1. 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2. 读取文件
        DataStreamSource<String> lingDSS = env.readTextFile("input/chapter02WordCount.txt");

        //3. 分词
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = lingDSS.flatMap((String line, Collector<String> words) -> {
            Arrays.stream(line.split(" ")).forEach(words::collect);
        }).returns(Types.STRING)
                .map(word -> Tuple2.of(word, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        //4. 分组
        KeyedStream<Tuple2<String, Long>, String> wordAndUG = wordAndOne.keyBy(t -> t.f0);

        //5. 求和
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = wordAndUG.sum(1);


        //6. 执行
        sum.print();

        //7. 执行
        env.execute();
    }

}
