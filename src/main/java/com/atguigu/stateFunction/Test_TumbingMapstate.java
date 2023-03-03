package com.atguigu.stateFunction;

import Function.MySourceFunction;
import bean.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author MengX
 * @create 2023/3/1 20:54:40
 */
public class Test_TumbingMapstate {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Event> sourceStream = env.addSource(new MySourceFunction());

        sourceStream.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
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
        }).process(new MyTumbingWindow(5000L))

                .print();

        env.execute();
    }

    public static class MyTumbingWindow extends KeyedProcessFunction<String, Event, String> {

        private MapState<Long, Integer> mapState;

        private Long windowSize;

        public MyTumbingWindow(Long windowSize) {

            this.windowSize = windowSize;
        }


        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {

            String currentKey = ctx.getCurrentKey();

            Long windowStart = timestamp + 1 - windowSize;

            Long windowEnd = timestamp + 1;

            out.collect("目前:"+currentKey+"在窗口时间："+windowStart+"~"+windowEnd+"中的个数是"+mapState.get(windowStart));

            mapState.remove(windowStart);


        }

        @Override
        public void open(Configuration parameters) throws Exception {

            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Integer>("myState", Long.class, Integer.class));

        }

        @Override
        public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {


            Long timeStamp = value.timeStamp;

            Long windowStart = timeStamp - (timeStamp + windowSize) % windowSize;

            Long windowEnd = windowStart + windowSize;


            if (mapState.contains(windowStart)) {

                Integer a = mapState.get(windowStart) + 1;

                mapState.put(windowStart, a);
            } else {

                mapState.put(windowStart, 1);
            }


            ctx.timerService().registerEventTimeTimer(windowEnd - 1l);

        }
    }
}
