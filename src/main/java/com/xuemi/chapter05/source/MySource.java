package com.xuemi.chapter05.source;

import com.xuemi.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * 自定义Source源类：
 *  1.继承SourceFunction接口
 *     2.重写run()——该方法主要是循环读取数据
 *     3.重写cancel()——该方法主要是停止数据的读取
 *  4.注意：应该指明SourceFunction接口的泛型具体类，和run方法的具体类
 */
public class MySource implements SourceFunction<Event> {

    //读取数据的标识
    private boolean flag = true;

    /**
     * 不断的读取数据
     * @param sourceContext：该对象是运行时上下文环境，可以往下游发送数据
     * @throws Exception
     */
    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {

        Random random = new Random(); // 在指定的数据集中随机选取数据
        String[] users = {"Mary", "Alice", "Bob", "Cary"};
        String[] urls = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};

        //循环读取数据
        while (flag) {
            //使用上下文环境对象向下游传送数据
            sourceContext.collect(
                    new Event(users[random.nextInt(users.length)],
                            urls[random.nextInt(urls.length)],
                            Calendar.getInstance().getTimeInMillis())
            );

            Thread.sleep(1000);
        }

    }

    /**
     * 将读取标识设置为false
     */
    @Override
    public void cancel() {
        flag = false;
    }
}
