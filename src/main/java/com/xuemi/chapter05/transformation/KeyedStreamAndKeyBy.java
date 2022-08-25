package com.xuemi.chapter05.transformation;

import com.xuemi.Event;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 接口KeySelector<IN, KEY> extends Function, Serializable：
 *  1.该接口两个类型，一个是输入类型，另一个是输出类型
 *  2.在getKey()方法中指明返回的key
 *
 *  调用keyBy()方法后得到的数据类型是KeyedStream，接口KeyedStream<T, KEY> extends DataStream<T>
 */
public class KeyedStreamAndKeyBy {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> source = env.fromElements(new Event("Jack", "/home", 4000L),
                new Event("Alice", "/home/game", 2000L),
                new Event("Tom", "/product/computer", 3000L),
                new Event("Alice", "/home/test", 2800L),
                new Event("Jack", "/home/game", 3500L),
                new Event("Alice", "/home/product/phone", 2000L),
                new Event("Tom", "/home/product/suite", 2000L),
                new Event("Jack", "/home/game/cjml", 2000L));

        KeyedStream<Event, String> keyBy = source.keyBy(new MyKeySelector());

        keyBy.min("timeStamp").print();
        keyBy.max("timeStamp").print();
        keyBy.sum("timeStamp").print();
        keyBy.maxBy("timeStamp").print();
        keyBy.minBy("timeStamp").print();

        env.execute();
    }

    public static class MyKeySelector implements KeySelector<Event, String> {

        @Override
        public String getKey(Event event) throws Exception {
            return event.user;//按照user进行分组
        }
    }

}
