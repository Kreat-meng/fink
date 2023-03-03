package com.atguigu;

import Function.MyRichMapFunction;

import bean.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.beans.Encoder;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

/**
 * @author MengX
 * @create 2023/2/24 20:07:25
 */
public class TestByRichFunction {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(3);

        //DataStreamSource<Event> mySource = env.addSource(new MySourceFunction());

        DataStreamSource<String> socketSource = env.socketTextStream("hadoop102", 9088);

        SingleOutputStreamOperator<Event> eventMap = socketSource.map(new MapFunction<String, Event>() {
            @Override
            public Event map(String s) throws Exception {

                String[] s1 = s.split(" ");

                return new Event(s1[0], s1[1], Long.parseLong(s1[2]));
            }
        });

        SingleOutputStreamOperator<Tuple3<String, String, String>> myMap =eventMap.map(new MyRichMapFunction());

        SingleOutputStreamOperator<String> stringMap = myMap.map(new MapFunction<Tuple3<String, String, String>, String>() {
            @Override
            public String map(Tuple3<String, String, String> stringStringStringTuple3) throws Exception {
                return stringStringStringTuple3.f0 + stringStringStringTuple3.f1 + stringStringStringTuple3.f2;
            }
        });

        StreamingFileSink<String> fileSink = StreamingFileSink.forRowFormat(new Path("./output"), new SimpleStringEncoder<String>())

                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(15))
                        .withMaxPartSize(1024*1024*1024)
                        .withRolloverInterval(TimeUnit.MINUTES.toMillis(15)).build())
                .build();

        stringMap.addSink(fileSink).setParallelism(1);

        env.execute();

    }
}
