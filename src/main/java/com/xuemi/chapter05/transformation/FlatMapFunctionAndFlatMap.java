package com.xuemi.chapter05.transformation;

import com.xuemi.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlatMapFunctionAndFlatMap {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);//设置全局并行度为1

        DataStreamSource<Event> source = env.fromElements(new Event("Jack", "/home", 1000L),
                new Event("Alice", "/home/game", 2000L),
                new Event("Tom", "/product/computer", 3000L),
                new Event("Alice", "/home/test", 2800L),
                new Event("Jack", "/home/game", 3500L),
                new Event("Alice", "/home/product/phone", 2000L),
                new Event("Tom", "/home/product/suite", 2000L),
                new Event("Jack", "/home/game", 2000L));

        //方式一，通过实现类
        SingleOutputStreamOperator<String> flatMap = source.flatMap(new MyFlatMapFunction());

        //方式一，通过匿名内部类
        SingleOutputStreamOperator<String> flatMap1 = source.flatMap(new FlatMapFunction<Event, String>() {
            @Override
            public void flatMap(Event event, Collector<String> out) throws Exception {
                out.collect(event.user);
                out.collect(event.url);
                out.collect(String.valueOf(event.timeStamp));
            }
        });

        //方式三，通过Lambda表达式
        SingleOutputStreamOperator<String> flatMap2 = source.flatMap((Event event, Collector<String> out) -> {
            out.collect(event.user);
            out.collect(event.url);
            out.collect(String.valueOf(event.timeStamp));
        }).returns(Types.STRING);//The return type of function 'main(FlatMapFunctionAndFlatMap.java:39)' could not be determined automatically, due to type erasure. You can give type information hints by using the returns(...) method on the result of the transformation call, or by letting your function implement the 'ResultTypeQueryable' interface.


//        flatMap.print();
//        flatMap1.print();
        flatMap2.print();

        env.execute();
    }

    public static class MyFlatMapFunction implements FlatMapFunction<Event, String> {

        /**
         * 输入一个event，输出三条字符串（分别是event的user、url、timestamp），即一个输入，压扁后多个输出
         * @param event
         * @param out
         * @throws Exception
         */
        @Override
        public void flatMap(Event event, Collector<String> out) throws Exception {
            //一个event，将其内部的属性压扁
            out.collect(event.user);
            out.collect(event.url);
            out.collect(String.valueOf(event.timeStamp));
        }
    }

}
