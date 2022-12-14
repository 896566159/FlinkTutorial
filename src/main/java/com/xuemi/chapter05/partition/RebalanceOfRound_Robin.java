package com.xuemi.chapter05.partition;

import com.xuemi.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 轮询分区（Round-Robin算法实现）：按照顺序依次向下游 taskSlot 均匀 的分发数据，
 * 调用rebalance()来轮询分区
 */
public class RebalanceOfRound_Robin {

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

        // 经轮询重分区后打印输出，并行度为 4
        source.rebalance().print("rebalance").setParallelism(4);

        env.execute();
    }

}
