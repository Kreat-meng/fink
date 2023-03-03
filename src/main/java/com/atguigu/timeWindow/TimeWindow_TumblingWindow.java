package com.atguigu.timeWindow;

import Function.MySourceFunction;
import bean.Event;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.HashSet;

/**
 * @author MengX
 * @create 2023/2/27 20:36:29
 */
public class TimeWindow_TumblingWindow {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Event> localhost = env.addSource(new MySourceFunction());



           localhost.keyBy(new KeySelector<Event, String>() {
               @Override
               public String getKey(Event event) throws Exception {

                   return "key";
               }
           }).window(TumblingProcessingTimeWindows.of(Time.seconds(5))).aggregate(new AggregateFunction<Event, Tuple2<HashSet, Long>, Double>() {
               @Override
               public Tuple2<HashSet, Long> createAccumulator() {

                   return Tuple2.of(new HashSet<String>(), 0l);
               }

               @Override
               public Tuple2<HashSet, Long> add(Event event, Tuple2<HashSet, Long> hashSetLongTuple2) {

                   hashSetLongTuple2.f0.add(event.name);

                   return Tuple2.of(hashSetLongTuple2.f0, hashSetLongTuple2.f1 + 1l);
               }

               @Override
               public Double getResult(Tuple2<HashSet, Long> hashSetLongTuple2) {


                   return (double) hashSetLongTuple2.f1 / hashSetLongTuple2.f0.size();
               }

               @Override
               public Tuple2<HashSet, Long> merge(Tuple2<HashSet, Long> hashSetLongTuple2, Tuple2<HashSet, Long> acc1) {
                   return null;
               }
           }, new ProcessWindowFunction<Double, String, String, TimeWindow>() {
               @Override
               public void process(String s, ProcessWindowFunction<Double, String, String, TimeWindow>.Context context, Iterable<Double> elements, Collector<String> out) throws Exception {

                   Timestamp start = new Timestamp(context.window().getStart());

                   Timestamp end = new Timestamp(context.window().getEnd());

                   Double pvTOuv = elements.iterator().next();

                   out.collect("窗口存在的时间"+start+"~"+end+",人均浏览量是："+ pvTOuv);


               }
           }).print();

           env.execute();
    }
}
