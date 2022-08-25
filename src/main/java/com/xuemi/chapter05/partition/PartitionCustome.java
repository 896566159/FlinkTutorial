package com.xuemi.chapter05.partition;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * .partitionCustom()可以自定义分区:
 *  1.需要传入两个参数：Partitioner<K>接口对象、KeySelector<IN,KEY>
 *  2.第一个对象是自定义分区器对象
 *  3.第二个参数是应用分区器它的指定方式与 keyBy 指定 key 基本一样：可以通过字段名称指定，
 *    也可以通过字段位置索引来指定，还可以实现一个 KeySelector。
 */
public class PartitionCustome {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromElements(1,2,3,4,5,6,7,8)
                .partitionCustom(new Partitioner<Integer>() {
                    @Override
                    public int partition(Integer key, int numPartitions) {
                        return key % 2;
                    }
                }, new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer value) throws Exception {
                        return value;
                    }
                }).print(".partition()方法中，要传入两个接口的实现类：Partitioner<K>和KeySelector<IN, KEY>")
                .setParallelism(2);

        env.execute();
    }

}
