package com.atguigu.testfunction;

import bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author MengX
 * @create 2023/3/4 16:40:24
 */
public class Test_unin {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<String> process = env.socketTextStream("localhost", 9999)

                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String s) throws Exception {

                        String[] s1 = s.split(" ");

                        return new WaterSensor(s1[0], Long.parseLong(s1[1]), Integer.parseInt(s1[2]));
                    }
                })

                .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                            @Override
                            public long extractTimestamp(WaterSensor waterSensor, long l) {

                                return waterSensor.getTs() * 1000;
                            }
                        })).process(new ProcessFunction<WaterSensor, String>() {
                    @Override
                    public void processElement(WaterSensor value,
                                               ProcessFunction<WaterSensor, String>.Context ctx,
                                               Collector<String> out) throws Exception {

                        String s = value.toString();

                        out.collect("1 流：" + s);

                        System.out.println("watermark1 :"+ ctx.timerService().currentWatermark());

                        System.out.println("===========================================================");

                    }
                });
        DataStream<String> stream1 = process;

        SingleOutputStreamOperator<String> process1 = env.socketTextStream("localhost", 7777)

                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String s) throws Exception {

                        String[] s1 = s.split(" ");

                        return new WaterSensor(s1[0], Long.parseLong(s1[1]), Integer.parseInt(s1[2]));
                    }
                })

                .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                            @Override
                            public long extractTimestamp(WaterSensor waterSensor, long l) {

                                return waterSensor.getTs() * 1000;
                            }
                        })).process(new ProcessFunction<WaterSensor, String>() {
                    @Override
                    public void processElement(WaterSensor value,
                                               ProcessFunction<WaterSensor, String>.Context ctx,
                                               Collector<String> out) throws Exception {

                        String s = value.toString();

                        out.collect("2 流：" + s);

                        System.out.println("watermark2 ：" + ctx.timerService().currentWatermark());

                        System.out.println("==========================================================");

                    }
                });

        process.union(process1).process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {

                System.out.println("unin Watermark: "+ctx.timerService().currentWatermark());

                out.collect("unin : " + value);
            }
        }).print();

        env.execute();
    }
}
