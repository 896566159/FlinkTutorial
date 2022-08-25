package com.xuemi.chapter05.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class SinkToKafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");

        /**
         * （1）添加 Kafka 连接器依赖
         * （2）启动 Kafka 集群
         * （3）编写输出到 Kafka 的示例代码
         */
        env.fromElements("1","2","3","4","5","6","7","8","9","0")
                .addSink(new FlinkKafkaProducer<String>(
                        "clicks",
                        new SimpleStringSchema(),
                        properties));

        env.execute();
    }

}
