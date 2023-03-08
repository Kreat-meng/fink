package com.atguigu.testfunction;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author MengX
 * @create 2023/3/6 14:55:31
 */
public class Test_IntervalJoin {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env
                = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStream<Tuple3<String, String, Long>> fromElements1
                = env.fromElements(
                        Tuple3.of("Songsong", "pv", 1000l),
                        Tuple3.of("Songsong", "ov", 4000l)
                        )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String,String,Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String,String,Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Long> stringStringLongTuple3, long l) {
                                return stringStringLongTuple3.f2;
                            }
                        }));


        DataStream<Tuple3<String, String, Long>> element2 = env.fromElements(
                Tuple3.of("Songsong", "pv", 1000L),
                Tuple3.of("Songsong", "pv", 3000L),
                Tuple3.of("Songsong", "pv", 2000L),
                Tuple3.of("Songsong", "pv", 5000L),
                Tuple3.of("Songsong", "pv", 6000L)

        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> stringStringLongTuple3, long l) {
                        return stringStringLongTuple3.f2;
                    }
                }));

        fromElements1.keyBy(new KeySelector<Tuple3<String, String, Long>, String>() {
            @Override
            public String getKey(Tuple3<String, String, Long> stringStringLongTuple3) throws Exception {
                return stringStringLongTuple3.f0;
            }
            // todo 间隔join操作
        }).intervalJoin(element2.keyBy(new KeySelector<Tuple3<String, String, Long>, String>() {
            @Override
            public String getKey(Tuple3<String, String, Long> stringStringLongTuple3) throws Exception {
                return stringStringLongTuple3.f0;
            }
        }))
                .between(Time.seconds(-2),Time.seconds(2))
                .process(new ProcessJoinFunction<Tuple3<String, String, Long>,
                        Tuple3<String, String, Long>, String>() {
                    @Override
                    public void processElement(Tuple3<String, String, Long> left,
                                               Tuple3<String, String, Long> right,
                                               ProcessJoinFunction<Tuple3<String, String, Long>,
                                                       Tuple3<String, String, Long>, String>.Context ctx,
                                               Collector<String> out) throws Exception {

                        out.collect(left.toString()+"->"+right.toString());

                    }
                }).print();
        env.execute();
    }
}
