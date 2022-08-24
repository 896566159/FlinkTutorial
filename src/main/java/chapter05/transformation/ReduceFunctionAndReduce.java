package chapter05.transformation;

import chapter05.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReduceFunctionAndReduce {

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

        //通过实现类来统计每个访问者的访问次数
        SingleOutputStreamOperator<Tuple2<String, Long>> reduce = source.map(new MapFunction<Event, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(Event event) throws Exception {
                return Tuple2.of(event.user, 1L);
            }
        })
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(value -> value.f0)
                .reduce(new MyReduceFunction());

//        source.map((Event event, Tuple2<String, Long> out) -> {
//            Tuple2.of(event.user, 1L);
//        })

        //使用Lambd表达式来完成：统计最近一次、 访问量最高的人
        //1.map()先将Event转换成（user, 1）
        //2.使用keyBy()按照user进行分组
        //3.reduce()对分组的数据进行求和，即求出每个user的访问量
        //4.keyBy()将分组后的数据使用ture作为键将所有的分组合成一个
        //5.reduce()找出这个大分组中：第二个元素最大的（即访问量）的元祖
        SingleOutputStreamOperator<Tuple2<String, Long>> reduce1 = source
                // 将 Event 数据类型转换成元组类型
                .map(event -> Tuple2.of(event.user, 1L))
                // 指明元祖的数据类型
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                // 使用用户名来进行分流
                .keyBy(value -> value.f0)
                // 每到一条数据，用户 pv 的统计值加 1
                .reduce(((value1, value2) -> {
                    return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                }))
                // 为每一条数据分配同一个 key，将聚合结果发送到一条流中
                .keyBy(value -> true)
                // 将 累加器更新为当前最大的 pv 统计值，然后向下游发送累加器的值
                .reduce(((value1, value2) -> {
                    return value1.f1 > value2.f1 ? value1 : value2;
                }));

//        reduce.print();
        reduce1.print();

        env.execute();
    }

    public static class MyReduceFunction implements ReduceFunction<Tuple2<String, Long>> {

        @Override
        public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
            return Tuple2.of(value1.f0, value1.f1 + value2.f1);
        }
    }
}
