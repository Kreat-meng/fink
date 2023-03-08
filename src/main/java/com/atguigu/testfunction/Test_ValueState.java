package com.atguigu.testfunction;

import bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author MengX
 * @create 2023/3/6 15:55:26
 */
public class Test_ValueState {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.socketTextStream("localhost",9999).map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String s) throws Exception {

                String[] s1 = s.split(" ");

                return new WaterSensor(s1[0],Long.parseLong(s1[1]),Integer.parseInt(s1[2]));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor waterSensor, long l) {
                        return waterSensor.getTs()*1000;
                    }
                })).keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor waterSensor) throws Exception {
                return waterSensor.getId();
            }
        }).process(new KeyedProcessFunction<String, WaterSensor, String>() {


            private ValueState<Integer> valueState;


            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("value_sate",Integer.class));
            }

            @Override
            public void processElement(WaterSensor value,
                                       KeyedProcessFunction<String, WaterSensor, String>.Context ctx,
                                       Collector<String> out) throws Exception {

                Integer lastVc = valueState.value() == null ? value.getVc() : valueState.value();

                if (Math.abs(value.getVc()-lastVc)>=10){

                    System.out.println("警告！！！超过水位线！！！");
                }else

                    valueState.update(value.getVc());

            }
        }).print();

        env.execute();

    }
}
