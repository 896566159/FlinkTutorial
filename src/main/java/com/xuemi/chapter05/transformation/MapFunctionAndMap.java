package com.xuemi.chapter05.transformation;

import com.xuemi.chapter05.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 接口：MapFunction<T, O> extends Function, Serializable
 *     1.T 输入的类型， O输出的类型
 *     2.map()方法，“一对一映射”，一个输入，得到一个输出
 */
public class MapFunctionAndMap {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> source = env.fromElements(new Event("Jack", "/home", 1000L),
                new Event("Alice", "/home/game", 2000L),
                new Event("Tom", "/product/computer", 3000L),
                new Event("Alice", "/home/test", 2800L),
                new Event("Jack", "/home/game", 3500L),
                new Event("Alice", "/home/product/phone", 2000L),
                new Event("Tom", "/home/product/suite", 2000L),
                new Event("Jack", "/home/game", 2000L));

        //方式一实现MapFunction接口的实现类
        SingleOutputStreamOperator<String> map = source.map(new MyMapFuntion());

        //方式二，通过匿名内部类，实现MapFunction接口
        SingleOutputStreamOperator<String> map1 = source.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event value) throws Exception {
                return value.url;//将其用户访问网址提取出来
            }
        });

        //方式三，通过Lambda表达式来创界MapFunction接口的实现类
        SingleOutputStreamOperator<String> map2 = source.map(event -> event.url);

//        map.print();
//        map1.print();
        map2.print();

        env.execute();
    }

    //方式一实现MapFunction接口的实现类
    public static class MyMapFuntion implements MapFunction<Event, String> {

        //在map()方法中写“映射”逻辑，如处理到每个Event时，将其用户名提取出来
        //在map()方法中写“映射”逻辑，如处理到每个Event时，将其用户访问网址提取出来
        @Override
        public String map(Event value) throws Exception {
//            return value.user;//将其用户名提取出来
            return value.url;//将其用户访问网址提取出来
        }
    }
    
}
