package com.atguigu;

import Function.MySourceFunction;
import bean.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author MengX
 * @create 2023/2/24 11:54:58
 */
public class Test_MyFunction {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStreamSource<Event> mySource = env.addSource(new MySourceFunction());//无法设置并行度 默认为1.setParallelism(2);

        mySource.print();

        env.execute();

    }
}
