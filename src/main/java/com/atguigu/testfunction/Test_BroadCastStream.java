package com.atguigu.testfunction;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author MengX
 * @create 2023/3/6 20:23:33
 */
public class Test_BroadCastStream {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> localhost = env.socketTextStream("localhost", 9999);

        DataStreamSource<String> hadoopStream = env.socketTextStream("localhost", 7777);


        MapStateDescriptor<String ,String> mapStateDescriptor = new MapStateDescriptor<String,String>("broad_map",String.class,String.class);
        //获取广播流
        BroadcastStream<String> broadcast = localhost.broadcast(mapStateDescriptor);

        //hadoop流链接广播流

        BroadcastConnectedStream<String, String> connect = hadoopStream.connect(broadcast);

        connect.process(new BroadcastProcessFunction<String, String, String>() {
            @Override
            public void processElement(String value,
                                       BroadcastProcessFunction<String, String, String>.ReadOnlyContext ctx,
                                       Collector<String> out) throws Exception {

                //广播规则
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                if ("1".equals(broadcastState.get("swich"))){

                    out.collect("执行加法逻辑=========");
                }else if ("2".equals(broadcastState.get("swich"))){

                    out.collect("执行减法逻辑=====================");
                }

            }

            @Override
            public void processBroadcastElement(String value,
                                                BroadcastProcessFunction<String, String, String>.Context ctx,
                                                Collector<String> out) throws Exception {

                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

                broadcastState.put("swich",value);

            }
        }).print();

        env.execute();

    }
}
