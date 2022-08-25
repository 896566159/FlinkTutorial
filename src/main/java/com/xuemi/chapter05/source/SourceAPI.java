package com.xuemi.chapter05.source;


import com.xuemi.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Properties;

public class SourceAPI {

    public static void main(String[] args) throws Exception {

        //1. 创建执行环境
        /**
         * getExecutionEnvironment()：可以自动识别将要创建的执行环境是本地环境还是远程环境
         * createLocalEnvironment(): 只可以创建本地的执行环境
         * createRemoteEnvironment(): 只可以创建远程的执行环境
         * addSource()：比较通用的Source源，可以从Kafka中读取数据出来
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();



        //2 获取数据——Source
        /**
         * readTextFile()：从文件读取数据
         * socketTextStream():从socket中读取数据
         * fromCollection()/fromElement()：从集合中读取数据
         *
         */
//        DataStreamSource<String> source = env.readTextFile("input/chapter05DataStream.txt");
//        DataStreamSource<String> source1 = env.socketTextStream("192.168.73.102", 7777);

        ArrayList<Event> list = new ArrayList<>();
        list.add(new Event("Jack","./hoem",1000L));
        list.add(new Event("Alis","./hoem",1000L));
        list.add(new Event("Tom","./hoem",1000L));
//        DataStreamSource<Event> source2 = env.fromCollection(list);


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
//        DataStreamSource<String> source3 = env.addSource(new FlinkKafkaConsumer<String>(
//                "clicks",
//                new SimpleStringSchema(),
//                properties
//        ));

        DataStreamSource<Event> source4 = env.addSource(new MySource());//自定义Source


        //3. 直接将source中输出
//        source.print("readTextFile");
//        source1.print("SocketTextFile");
//        source2.print("Collection");
//        source3.print("kafka");
        source4.print("MySource");


        //4. 执行（一定要在此调用execute是因为main程序启动之后，并非数据也立即过来，等数据来了之后才能真的开始运行）
        env.execute();
    }

}
