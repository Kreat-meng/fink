package com.atguigu;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author MengX
 * @create 2023/2/21 19:41:07
 */
public class Stream_BoundedWordcount {

    public static void main(String[] args) throws Exception {

        /**
         * spark中求WordCount逻辑
         * 1.创建Spark环境
         * 2.用读取文件的算子将文件中的数据读取到 RDD
         * 3.调用FlatMap算子 对数据进行转换 转换为Tuple2元组（单词，1）
         * 4.调用reduceBykey算子，先对相同单词的数据聚合到一快 然后根据reduceByKey中的逻辑做累加
         * 5.调用行动算子输出
         * 6.释放资源
         */

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度为1
        executionEnvironment.setParallelism(1);

        DataStreamSource<String> readStream = executionEnvironment.readTextFile("input/wordcount.txt");

        SingleOutputStreamOperator<Tuple2<String, Integer>> flapMapWord = readStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {

                String[] words = s.split(" ");

                for (String word : words) {

                    Tuple2<String, Integer> tuple2 = Tuple2.of(word, 1);

                    collector.collect(tuple2);
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> wordOne = flapMapWord.keyBy("f0");

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = wordOne.sum("f1");

        sum.print();

        // 流处理必须提交这个job，每一个 execute 就是一个job
        executionEnvironment.execute();
    }
}
