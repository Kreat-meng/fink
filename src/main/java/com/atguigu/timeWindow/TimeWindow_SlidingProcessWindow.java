package com.atguigu.timeWindow;

import bean.Event;
import bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * @author MengX
 * @create 2023/2/28 09:45:28
 */
public class TimeWindow_SlidingProcessWindow {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        env.setParallelism(1);

        DataStreamSource<String> localhost = env.socketTextStream("localhost", 9999);

        localhost.process(new ProcessFunction<String, Event>() {
            @Override
            public void processElement(String value, ProcessFunction<String, Event>.Context ctx, Collector<Event> out) throws Exception {


                String[] s = value.split(" ");

                out.collect(new Event(s[0],s[1],Long.parseLong(s[2])));
            }
        }).keyBy(new KeySelector<Event, String>() {
                    @Override
                    public String getKey(Event event) throws Exception {
                        return "aaa";
                    }
                })/*.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))*/
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(5)))
                .aggregate(new AggregateFunction<Event, HashMap<String, Integer>, ArrayList<Tuple2<String,Integer>>>() {

            //private HashMap<String, Integer> stringIntegerHashMap = new HashMap<>();

            @Override
            public HashMap<String, Integer> createAccumulator() {

                HashMap<String, Integer> stringIntegerHashMap = new HashMap<>();
                //System.out.println(stringIntegerHashMap);
                //System.out.println("1111");

                //System.out.println("------------------------------------------------");



                return stringIntegerHashMap;

            }

            @Override
            public HashMap<String, Integer> add(Event event, HashMap<String, Integer> stringIntegerHashMap) {

                System.out.println(event);

                System.out.println("lalalalal");

                if (stringIntegerHashMap.containsKey(event.url)) {

                    stringIntegerHashMap.put(event.url, stringIntegerHashMap.get(event.url) + 1);
                } else {

                    stringIntegerHashMap.put(event.url, 1);
                }
                return stringIntegerHashMap;
            }

            @Override
            public ArrayList<Tuple2<String,Integer>> getResult(HashMap<String, Integer> stringIntegerHashMap) {

                ArrayList<Tuple2<String, Integer>> tuple2s = new ArrayList<>();


                Set<Map.Entry<String, Integer>> entries = stringIntegerHashMap.entrySet();

                for (Map.Entry<String, Integer> entry : entries) {

                   tuple2s.add(Tuple2.of(entry.getKey(),entry.getValue()));
                }

                return tuple2s;
            }

            @Override
            public HashMap<String, Integer> merge(HashMap<String, Integer> stringIntegerHashMap, HashMap<String, Integer> acc1) {
                return null;
            }
        }, new ProcessWindowFunction<ArrayList<Tuple2<String,Integer>>,String, String, TimeWindow>() {

                    @Override
                    public void process(String str, ProcessWindowFunction<ArrayList<Tuple2<String, Integer>>, String, String, TimeWindow>.Context context, Iterable<ArrayList<Tuple2<String, Integer>>> elements, Collector<String> out) throws Exception {

                        long start = context.window().getStart();

                        long end = context.window().getEnd();

                        ArrayList<Tuple2<String, Integer>> element = elements.iterator().next();

                        element.sort(new Comparator<Tuple2<String, Integer>>() {
                                @Override
                                public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {

                                    return o2.f1 - o1.f1;
                                }
                            });

                        System.out.println("11111111");
                        System.out.println(element);
                        //out.collect("在窗口时间："+start/1000+"~"+end/1000+"中"+"前两名的浏览url和浏览量分别是："+ element.get(0) + element.get(1));


                    }
                }).print();

        env.execute();
    }
}
