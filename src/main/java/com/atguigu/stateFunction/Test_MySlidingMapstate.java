package com.atguigu.stateFunction;

import Function.MySourceFunction;
import bean.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Time;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;

/**
 * @author MengX
 * @create 2023/3/2 14:44:45
 */
public class Test_MySlidingMapstate {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.//socketTextStream("localhost",9999)
               addSource(new MySourceFunction())

        //streamSource*/
                /*.map(new MapFunction<String, Event>() {
                    @Override
                    public Event map(String s) throws Exception {

                        String[] s1 = s.split(" ");

                        return new Event(s1[0],s1[1],Long.parseLong(s1[2]));
                    }
                })*/
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timeStamp;
                            }
                        })).keyBy(new KeySelector<Event, String>() {
                    @Override
                    public String getKey(Event event) throws Exception {
                        return event.url;
                    }
                }).process(new MySlidingWindow(6000l,3000l))

                .print();

        env.execute();
    }

    public static class MySlidingWindow extends KeyedProcessFunction<String,Event,String>{

        private Long windowsize;

        private Long windowshild;

        private MapState<Long,Integer> mapState;

        private ArrayList<Long> windowList;

        public MySlidingWindow(Long windowsize,Long windowshild){

            this.windowshild = windowshild;

            this.windowsize = windowsize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {

            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<Long,Integer>("slidingMapstate",
                    Long.class,Integer.class));
        }



        @Override
        public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx,
                                   Collector<String> out) throws Exception {

            Long lastWindowStart = value.timeStamp - (value.timeStamp + windowshild) % windowshild;

            for (long start = lastWindowStart; start > value.timeStamp - windowsize; start -= windowshild) {

                if (mapState.contains(start)){

                    //System.out.println(start);

                    Integer a = mapState.get(start) + 1;

                    mapState.put(start,a);
                    //System.out.println(mapState.get(start));

                }else {

                    mapState.put(start,1);

                    Long end = start + windowsize;


                    ctx.timerService().registerEventTimeTimer(end-1l);
                }

            }

        }


        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event,String>.OnTimerContext ctx,
                            Collector<String> out) throws Exception {

            String currentKey = ctx.getCurrentKey();

            //System.out.println(timestamp);

            System.out.println("watermark:"+ctx.timerService().currentWatermark()/1000);

            Long start = timestamp + 1 - windowsize ;

            Long end = timestamp + 1  ;
            //System.out.println(mapState.get(start));

            out.collect("目前:"+currentKey+"在窗口时间："+start/1000+"~"+end/1000
                    +"中的个数是"+mapState.get(start));

            mapState.remove(start);



        }
    }
}
