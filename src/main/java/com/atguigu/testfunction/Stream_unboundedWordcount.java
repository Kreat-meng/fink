package com.atguigu.testfunction;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author MengX
 * @create 2023/2/21 19:56:47
 */
public class Stream_unboundedWordcount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> steamHa = env.socketTextStream("hadoop102", 9088);

        SingleOutputStreamOperator<Tuple2<String, Integer>> flamap = steamHa.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {

                String[] s1 = s.split(" ");

                for (String s2 : s1) {

                    Tuple2<String, Integer> wordTuple = Tuple2.of(s2, 1);

                    collector.collect(wordTuple);
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> wordOne =
                flamap.keyBy((KeySelector<Tuple2<String, Integer>, String>)
                        stringIntegerTuple2 -> stringIntegerTuple2.f0);


        // fink中lomada 表达式类型擦除 解决

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = wordOne.sum("f1");

        sum.print();

        env.execute();


    }
}
