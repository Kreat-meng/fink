package com.atguigu.testfunction;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @author MengX
 * @create 2023/3/4 17:37:37
 */
public class StreamCounet {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Integer> elementStream1 = env.fromElements(1, 2, 3, 4, 5, 6);

        DataStreamSource<String> elementStream2 = env.fromElements("a", "b", "c", "d", "e", "f");

        elementStream1.connect(elementStream2).map(new CoMapFunction<Integer, String, Tuple2<Integer,String>>() {

            List<Tuple2<Integer,String>> list = new ArrayList<>();

            Tuple2<Integer,String> tuple2 = new Tuple2<>();

            @Override
            public Tuple2<Integer, String> map1(Integer value) throws Exception {

                Tuple2<Integer, String> of = Tuple2.of(value, "");

                list.add(of);

                return of;

            }

            @Override
            public Tuple2<Integer, String> map2(String value) throws Exception {

                Tuple2<Integer, String> of = Tuple2.of(0, value);

                for (Tuple2<Integer, String> integerStringTuple2 : list) {

                    System.out.println(integerStringTuple2.f0);
                }
                return of;

            }
        }).print();

        env.execute();
    }
}
