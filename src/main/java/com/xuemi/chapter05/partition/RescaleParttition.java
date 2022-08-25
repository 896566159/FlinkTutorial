package com.xuemi.chapter05.partition;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class RescaleParttition {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new RichParallelSourceFunction<Integer>() {
            @Override
            public void run(SourceContext<Integer> ctx) throws Exception {
                for (int i = 0; i < 8; i++) {
                    if (i % 2 == getRuntimeContext().getIndexOfThisSubtask()) {//如果数据取余等于当前的taskslot的索引，则将当前数据流入到该taskslot中
                        ctx.collect(i);//当前taskSlot对该数据进行采集
                    }
                }
            }

            @Override
            public void cancel() {

            }
        }).setParallelism(2)//设置Source的并行度为2，即开启两个读取数据的taskSlot
                .rescale()//对Source进行数据源重缩放分区
        .print("rescale")
                .setParallelism(4);//设置打印并行度为4，则一个Source有两个print，每个Source面向两个print分发数据

        env.execute();
    }

}
