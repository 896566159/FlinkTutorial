package com.xuemi.chapter06.watermarks;

import com.xuemi.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.Calendar;
import java.util.Random;

/**
 * 在Source中增加水位线
 */
public class WatermarkGeneratorInSource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> source = env.addSource(new MySourceWithWatermarkGenerator());

        source.print();

        env.execute();
    }


    public static class MySourceWithWatermarkGenerator implements SourceFunction<Event> {

        boolean running = true;//读取数据标志位

        @Override
        public void run(SourceContext<Event> sourceContext) throws Exception {
            Random random = new Random();
            String[] userArr = {"Mary", "Bob", "Alice"};
            String[] urlArr = {"./home", "./cart", "./prod?id=1"};
            while (running) {
                long currTs = Calendar.getInstance().getTimeInMillis(); // 毫秒时间戳
                String username = userArr[random.nextInt(userArr.length)];
                String url = urlArr[random.nextInt(urlArr.length)];
                Event event = new Event(username, url, currTs);

                // 使用 collectWithTimestamp 方法将数据发送出去，并指明数据中的时间戳的字段
                sourceContext.collectWithTimestamp(event, event.timeStamp);

                //发送水位线
                sourceContext.emitWatermark(new Watermark(event.timeStamp - 1L));

                Thread.sleep(1000L);//一秒钟产生一个Event事件
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
