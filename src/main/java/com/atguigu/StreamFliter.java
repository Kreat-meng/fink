package com.atguigu;

import Function.MySourceFunction;
import bean.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.zookeeper3.org.apache.jute.compiler.JString;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author MengX
 * @create 2023/2/24 16:33:31
 */
public class StreamFliter {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Event> mySource = env.addSource(new MySourceFunction());

        SingleOutputStreamOperator<Tuple2<String, Integer>> mapToTuple2 = mySource.map(new MapFunction<Event, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Event event) throws Exception {

                return new Tuple2<String, Integer>(event.name, 1);
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> filterData = mapToTuple2.filter(new FilterFunction<Tuple2<String, Integer>>() {
            @Override
            public boolean filter(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {


                return !stringIntegerTuple2.f0.equals("sisi");
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> tuple2Key = filterData.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {


                return stringIntegerTuple2.f0;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = tuple2Key.sum(1);


        // TODO 求所有用户中访问的最多的人

        KeyedStream<Tuple2<String, Integer>, String> keyFinal = sum.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return "a";
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> maxf = keyFinal.maxBy("f1",false);

        maxf.print();

        env.execute();

    }
}
