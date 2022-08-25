package com.xuemi.chapter05.sink;

import com.xuemi.Event;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

public class SinkToFile {

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

        /**
         * 这里我们创建了一个  简单的文件 Sink，通过.withRollingPolicy()方法指定了一个“滚动策
         * 略”。“滚动”的概念在日志文件的写入中经常遇到：因为文件会有内容持续不断地写入，所以
         * 我们应该给一个标准，到什么时候就开启新的文件，将之前的内容归档保存。也就是说，上面
         * 的代码设置了在以下 3 种情况下，我们就会滚动分区文件：
         *  ⚫ 至少包含 15 分钟的数据
         *  ⚫ 最近 5 分钟没有收到新的数据
         *  ⚫ 文件大小已达到 1 GB
         */
        StreamingFileSink<String> fileSink = StreamingFileSink.<String>forRowFormat(new Path("./output/chapter05fileSink_output"),
                new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(15))
                        .withRolloverInterval(TimeUnit.MILLISECONDS.toMillis(5))
                        .withMaxPartSize(1024 * 1024 * 1024)
                        .build())
                .build();

        //将Event转成String写入文件
//        source.map(envent -> envent.toString()).addSink(fileSink);
        source.map(Event::toString).addSink(fileSink);

        env.execute();
    }

}
