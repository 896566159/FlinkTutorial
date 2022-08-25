package com.xuemi.chapter06.watermarks;

import com.xuemi.chapter05.Event;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 自定义周期性水位线生成器
 */
public class MyPeriodicWatermarksGenerator {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> sorce = env.fromElements(new Event("Jack", "/home", 4000L),
                new Event("Alice", "/home/game", 2000L),
                new Event("Tom", "/product/computer", 3000L),
                new Event("Alice", "/home/test", 2800L),
                new Event("Jack", "/home/game", 3500L),
                new Event("Alice", "/home/product/phone", 2000L),
                new Event("Tom", "/home/product/suite", 2000L),
                new Event("Jack", "/home/game/cjml", 2000L));

        sorce.assignTimestampsAndWatermarks(new MyWatermarkStrategy())
                .print();

        env.execute();
    }

    public static class MyWatermarkStrategy implements WatermarkStrategy<Event> {

        @Override
        public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new MyPeriodicGenerator();
        }

        @Override
        public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new SerializableTimestampAssigner<Event>() {
                @Override
                public long extractTimestamp(Event element, long recordTimestamp) {
                    return element.timeStamp;// 告诉程序数据源里的时间戳是哪一个字段
                }
            };
        }
    }

    public static class MyPeriodicGenerator implements WatermarkGenerator<Event> {

        private Long delayTime = 5000L; // 延迟时间
        private Long maxTs = Long.MIN_VALUE + delayTime + 1L; // 观察到的最大时间戳，之前流水中的最大时间戳

        @Override
        public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
            // 每来一条数据就调用一次
            maxTs = Math.max(event.timeStamp, maxTs); // 更新最大时间戳
        }

        /**
         * output.emitWatermark()，就可以发出水位线了；这个方法
         * 由系统框架周期性地调用，默认 200ms 一次。所以水位线的时间戳是依赖当前已有数据的最
         * 大时间戳的（这里的实现与内置生成器类似，也是减去延迟时间再减 1），但具体什么时候生成与数据无关
         * @param output
         */
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            //发送水位线，默认200ms发送一次
            //发送出去的水位线：maxTs - delayTime - 1L，可以确保窗口是左闭右开的[)
            //即水位线为 400， 表示 时间戳小于等于400的数据已经全部发送齐全
            output.emitWatermark(new Watermark(maxTs - delayTime - 1L));
        }
    }

}
