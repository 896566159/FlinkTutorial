package com.xuemi.chapter05.partition;

import com.xuemi.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * broadcast()广播分区会将数据广播发送到下游的所有TaskSlot中，数据会被重复消费
 */
public class BroadcastPartition {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> source = env.fromElements(new Event("Jack", "/home", 4000L),
                new Event("Alice", "/home/game", 2000L),
                new Event("Tom", "/product/computer", 3000L),
                new Event("Alice", "/home/test", 2800L),
                new Event("Jack", "/home/game", 3500L),
                new Event("Alice", "/home/product/phone", 2000L),
                new Event("Tom", "/home/product/suite", 2000L),
                new Event("Jack", "/home/game/cjml", 2000L));

        //经过广播分区后，设置并行度为4的打印
        source.broadcast().print("broadcast").setParallelism(4);

        env.execute();
    }
}
