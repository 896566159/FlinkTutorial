package com.xuemi.chapter06.watermarks;

import com.xuemi.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class TimestampsAndWatermarks {

    public static void main(String[] args) throws Exception {

        //DataStream API中有一个.assignTimestampsAndWatermarks()方法：assignTimestampsAndWatermarks(WatermarkStrategy<T> watermarkStrategy)
        //1.提取出时间戳——数据本身就有一个时间戳，但是需要让 DataStream 知道该数据的时间戳在什么字段中
        //2.生成水位线 watermarks 来更新事件时钟
        //3.方法中需要传入参数 WatermarkStrategy<T> 接口的对象，使用时一般调用静态方法来获取到 WatermarkStrategy 对象，另外，WatermarkStrategy<T>中包含两个接口对象
            //3.0 WatermarkStrategy<T> extends TimestampAssignerSupplier<T>, WatermarkGeneratorSupplier<T>
            //3.1 在代码中一般使用 WatermarkStrategy 接口的静态方法.<T>forMonotonousTimestamps() 或者 <T>forBoundedOutOfOrderness(Duration maxOutOfOrderness) 来获取到 WatermarkStrategy 对象
                // 3.1.1 其中.<T>forMonotonousTimestamps()，用于有序流中，一般在生产环境中很少用
                // 3.1.2 .<T>forBoundedOutOfOrderness(Duration maxOutOfOrderness)，用于乱序流，需要传入Duration对象，该对象一般也使用Duration的静态方法来获取对象。该Duration对象中有指明了事件时钟的延迟时间
            //3.1 TimestampAssigner<T>——时间戳分配器:主要负责从流中数据元素的某个字段中提取时间戳，并分配给元素。时间戳的分配是生成水位线的基础。
            //3.2 WatermarkGenerator<T>——水位线生成器:主要负责按照既定的方式，基于时间戳生成水位线。在 WatermarkGenerator 接口中，主要又有两个方法：onEvent()和 onPeriodicEmit()。
                //3.2.1 onEvent(T event, long eventTimestamp, WatermarkOutput output):每个事件（数据）到来都会调用的方法，它的参数有当前事件、时间戳，以及允许发出水位线的一个 WatermarkOutput，可以基于事件做各种操作
                //3.2.2 onPeriodicEmit(WatermarkOutput output):周期性调用的方法，可以由 WatermarkOutput 发出水位线。周期时间为处理时间，可以调用环境配置的.setAutoWatermarkInterval()方法来设置，默认为200ms。env.getConfig().setAutoWatermarkInterval(60 * 1000L);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> sorce = env.fromElements(new Event("Jack", "/home", 4000L),
                new Event("Alice", "/home/game", 2000L),
                new Event("Tom", "/product/computer", 3000L),
                new Event("Alice", "/home/test", 2800L),
                new Event("Jack", "/home/game", 3500L),
                new Event("Alice", "/home/product/phone", 2000L),
                new Event("Tom", "/home/product/suite", 2000L),
                new Event("Jack", "/home/game/cjml", 2000L));

//        SingleOutputStreamOperator<Event> assignTimestampsAndWatermarks = sorce.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
//                //有序流
//                //.withTimestampAssigner()方法，将数据中的 timestamp 字段提取出来，
//                //作为时间戳分配给数据元素；然后用内置的有序流水位线生成器构造出了生成策略。这样，提
//                //取出的数据时间戳，就是我们处理计算的事件时间。
//                //这里需要注意的是，时间戳和水位线的单位，必须都是毫秒
//                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
//                    @Override
//                    public long extractTimestamp(Event event, long recordTimestamp) {
//                        return event.timeStamp;//指明event对象中的什么字段可以提取来作为时间戳
//                    }
//                }));

        //一般无序流使用较多，有序流 == 无序流的延迟时间设置为 0
        //WatermarkStrategy.forMonotonousTimestamps() == WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(0))
        //
        SingleOutputStreamOperator<Event> assignTimestampsAndWatermarks = sorce.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(
                Duration.ofSeconds(5)//事件时钟延迟5秒钟
        ).withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            // 抽取时间戳的逻辑
            @Override
            public long extractTimestamp(Event element, long recordTimestamp) {
                return element.timeStamp;//指明事件中的什么字段可以被提取来作为时间戳
            }
        }));

        env.execute();

    }

}
