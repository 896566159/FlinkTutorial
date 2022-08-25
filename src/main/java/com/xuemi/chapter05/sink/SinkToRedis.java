package com.xuemi.chapter05.sink;

import com.xuemi.Event;
import com.xuemi.chapter05.source.MySource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class SinkToRedis {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建一个到 redis 连接的配置
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("hadoop102").build();

        /**
         * 这里 RedisSink 的构造方法需要传入两个参数：
         * ⚫ JFlinkJedisConfigBase：Jedis 的连接配置
         * ⚫ RedisMapper：Redis 映射类接口，说明怎样将数据转换成可以写入 Redis 的类型
         */
        env.addSource(new MySource())
                .addSink(new RedisSink<Event>(conf, new MyRedisMapper()));
        env.execute();
    }

    //接下来主要就是定义一个 Redis 的映射类，实现 RedisMapper 接口。
    public static class MyRedisMapper implements RedisMapper<Event> {
        @Override
        public String getKeyFromData(Event e) {
            return e.user;
        }

        @Override
        public String getValueFromData(Event e) {
            return e.url;
        }

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "clicks");
        }
    }

}
