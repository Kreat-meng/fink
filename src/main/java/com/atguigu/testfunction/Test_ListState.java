package com.atguigu.testfunction;

import Function.MySourceFunction;
import bean.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * @author MengX
 * @create 2023/3/6 16:21:08
 */
public class Test_ListState {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.addSource(new MySourceFunction()).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timeStamp;
                            }
                        })
        ).keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event event) throws Exception {
                return event.url;
            }
            // todo 求每个url中前三访问的用户是谁和访问次数；（不适合用liststate状态来写）
        }).process(new KeyedProcessFunction<String, Event, String>() {

            ListState<Tuple2<String, Integer>> listState;

            HashMap<String, Integer> presons;

            ArrayList<Map.Entry<String, Integer>> entries;


            @Override
            public void open(Configuration parameters) throws Exception {
                listState = getRuntimeContext().getListState(
                        new ListStateDescriptor<Tuple2<String, Integer>>("list_state", Types.TUPLE(Types.STRING,Types.INT)) {
                        });

                presons = new HashMap<>();

                entries = new ArrayList<>();


            }

            @Override
            public void processElement(Event value,
                                       KeyedProcessFunction<String, Event, String>.Context ctx,
                                       Collector<String> out) throws Exception {


                if (presons.containsKey(value.name)) {

                    Integer sum = presons.get(value.name) + 1;

                    presons.put(value.name, sum);
                } else

                    presons.put(value.name, 1);

                for (Map.Entry<String, Integer> entry : presons.entrySet()) {


                    entries.add(entry);

                }



                if (entries.size() <=3){

                    for (Map.Entry<String, Integer> entry : entries) {

                        listState.add(Tuple2.of(entry.getKey(),entry.getValue()));
                    }

                    out.collect(ctx.getCurrentKey()+"； "+listState.get().toString());

                    listState.clear();

                }else if (entries.size()>=4){

                    entries.sort(new Comparator<Map.Entry<String, Integer>>() {
                        @Override
                        public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                            return o2.getValue() - o1.getValue();
                        }
                    });

                    listState.add(Tuple2.of(entries.get(0).getKey(),entries.get(0).getValue()));
                    listState.add(Tuple2.of(entries.get(1).getKey(),entries.get(1).getValue()));
                    listState.add(Tuple2.of(entries.get(2).getKey(),entries.get(2).getValue()));


                    out.collect(ctx.getCurrentKey()+": "+listState.get().toString());

                    listState.clear();
                }

            }
        }).print();

        env.execute();
    }
}
