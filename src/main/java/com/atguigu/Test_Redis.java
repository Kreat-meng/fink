package com.atguigu;

import Function.MyRedisSink;
import bean.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author MengX
 * @create 2023/2/27 19:09:46
 */
public class Test_Redis {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> localhost = env.socketTextStream("localhost", 9999);

        localhost.map(new MapFunction<String, Event>() {
            @Override
            public Event map(String s) throws Exception {

                String[] s1 = s.split(" ");

                return new Event(s1[0],s1[1],Long.parseLong(s1[2]));
            }
        }).addSink(new MyRedisSink<Event>());

        env.execute();
    }
}
