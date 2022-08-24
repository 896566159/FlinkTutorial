package com.xuemi.chapter02WordCount;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class StreamWordCount {

    public static void main(String[] args) throws Exception{
        //1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2. 监听端口
        DataStreamSource<String> lineSteam = env.socketTextStream("192.168.73.102", 7777);

        //3. 对每一行做分词
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = lineSteam.flatMap((String line, Collector<String> words) -> {
            Arrays.stream(line.split(" ")).forEach(words::collect);
        }).returns(Types.STRING)
                .map(word -> Tuple2.of(word, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        //4. 分组
        KeyedStream<Tuple2<String, Long>, String> wordsGroup = wordAndOne.keyBy(t -> t.f0);

        //5. 求和
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = wordsGroup.sum(1);

        //6. 打印
        sum.print();

        //7. 执行
        env.execute();
    }

}
