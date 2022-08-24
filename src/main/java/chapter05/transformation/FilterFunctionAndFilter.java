package chapter05.transformation;


import chapter05.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * 接口FilterFunction<T> extends Function, Serializable
 *  1.T 是输入的数据类型
 *  2.在filter()方法中写出具体的筛选数据的条件
 */
public class FilterFunctionAndFilter {

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

        //方式一，通过实现类
        SingleOutputStreamOperator<Event> filter = source.filter(new MyFilterFunction());

        //方式一，通过匿名内部类实现
        SingleOutputStreamOperator<Event> filter1 = source.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                return value.url.equals("/home/game");//筛选出 访问了/home/game 的浏览事件
            }
        });

        //方式一，通过Lambda表达式
        SingleOutputStreamOperator<Event> filter2 = source.filter(event -> event.url.equals("/home/game"));


//        filter.print();
//        filter1.print();
        filter2.print();

        env.execute();
    }

    public static class MyFilterFunction implements FilterFunction<Event> {

        @Override
        public boolean filter(Event value) throws Exception {
//            return value.user.equals("Jack");//筛选出 Jack 的浏览事件
            return value.url.equals("/home/game");//筛选出 访问了/home/game 的浏览事件
        }
    }

}
