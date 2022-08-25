package com.xuemi.chapter06.watermarks;

import com.xuemi.Event;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MyPunctuatedWatermarkGenerator {

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

        sorce.assignTimestampsAndWatermarks(new MyPunctuatedWatermarkStrategy());

        env.execute();
    }

    public static class MyPunctuatedWatermarkStrategy implements WatermarkStrategy<Event> {

        @Override
        public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new MyPunctuatedGenerator();
        }

        @Override
        public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new SerializableTimestampAssigner<Event>() {
                @Override
                public long extractTimestamp(Event event, long recordTimestamp) {
                    return event.timeStamp;//告诉Flink时间戳是哪个字段
                }
            };
        }
    }

    public static class MyPunctuatedGenerator implements WatermarkGenerator<Event> {

        private Long delayTime = 5000L; // 延迟时间
        private Long maxTs = Long.MIN_VALUE + delayTime + 1L; // 观察到的最大时间戳

        @Override
        public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
            // 只有在遇到特定的 itemId 时，才发出水位线
            if (event.user.equals("Mary")) {
                output.emitWatermark(new Watermark(event.timeStamp - 1));
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // 不需要做任何事情，因为我们在 onEvent 方法中发射了水位线
        }
    }

}
