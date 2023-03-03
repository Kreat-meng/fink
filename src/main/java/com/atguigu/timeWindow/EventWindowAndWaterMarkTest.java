package com.atguigu.timeWindow;

import bean.WaterSensor;
import com.google.gson.internal.bind.util.ISO8601Utils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author MengX
 * @create 2023/3/1 11:20:34
 */
public class EventWindowAndWaterMarkTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> localhost = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<String> aggregateResult = localhost.map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String s) throws Exception {

                        String[] s1 = s.split(" ");

                        return new WaterSensor(s1[0], Long.parseLong(s1[1]), Integer.parseInt(s1[2]));
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        //设置watermark延迟时间（数据乱序程度）
                            //创建watermar生成器
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                            //创建时间戳分配器
                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                            @Override
                            public long extractTimestamp(WaterSensor waterSensor, long l) {
                                return waterSensor.getTs()*1000;
                            }
                        })).keyBy("id")
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                //允许窗口数据迟到的时间
                .allowedLateness(Time.seconds(3))
                //为迟到数据开启测流
                .sideOutputLateData(new OutputTag<WaterSensor>("output") {
                })
                .aggregate(new AggregateFunction<WaterSensor, Tuple2<String, Integer>, String>() {


                    private Integer sum;

                    @Override
                    public Tuple2<String, Integer> createAccumulator() {
                        sum = 0;
                        return Tuple2.of(null, sum);
                    }

                    @Override
                    public Tuple2<String, Integer> add(WaterSensor waterSensor, Tuple2<String, Integer> stringIntegerTuple2) {

                        System.out.println(stringIntegerTuple2);

                        stringIntegerTuple2.f0 = waterSensor.getId();

                        sum+=1;

                        System.out.println(sum);

                        stringIntegerTuple2.f1 = sum;

                        return stringIntegerTuple2;
                    }

                    @Override
                    public String getResult(Tuple2<String, Integer> stringIntegerTuple2) {

                        return stringIntegerTuple2.f0 + ":" + "count=" + stringIntegerTuple2.f1;
                    }

                    @Override
                    public Tuple2<String, Integer> merge(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> acc1) {
                        return null;
                    }
                }, new ProcessWindowFunction<String, String, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple, ProcessWindowFunction<String, String, Tuple, TimeWindow>.Context context, Iterable<String> elements, Collector<String> out) throws Exception {

                        String next = elements.iterator().next();

                        long start = context.window().getStart();

                        long end = context.window().getEnd();

                        out.collect("窗口时间："+start/1000+"~"+end/1000+"中"+next);
                    }
                });

        aggregateResult.print("主流");

        aggregateResult.getSideOutput(new OutputTag<WaterSensor>("output"){}).print("测流");

        env.execute();
    }
}
