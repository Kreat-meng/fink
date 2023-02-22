package com.atguigu;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author MengX
 * @create 2023/2/21 18:52:36
 */
public class Batch_worldcount {

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

        // 创建fink批处理环境

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 调用方法读取文件
        DataSource<String> stringDataSource = env.readTextFile("input/wordcount.txt");

        FlatMapOperator<String, Tuple2<String, Integer>> flatMap = stringDataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {

                String[] s1 = s.split(" ");

                for (String s2 : s1) {

                    Tuple2<String, Integer> tuple2 = Tuple2.of(s2, 1);

                    collector.collect(tuple2);
                }
            }
        });

        /*UnsortedGrouping<Tuple2<String, Integer>> wordOne = flatMap.groupBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0;
            }
        });*/

        UnsortedGrouping<Tuple2<String, Integer>> wordOne = flatMap.groupBy(0);


        AggregateOperator<Tuple2<String, Integer>> sum = wordOne.sum(1);


        sum.print();


    }
}
