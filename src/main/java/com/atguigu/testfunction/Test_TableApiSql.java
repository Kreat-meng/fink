package com.atguigu.testfunction;

import bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author MengX
 * @create 2023/3/7 19:42:43
 */
public class Test_TableApiSql {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //获取数据源

        DataStreamSource<WaterSensor> watersensorStream = env.fromElements(
                new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60)

        );

        //创建表环境

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //流转换为动态查询表 table对象

        Table table = tableEnv.fromDataStream(watersensorStream);

        // todo 追加流的情况 ：
        //利用table对象对数据进行操作

/*        Table resultTable = table
                .where($("id").isEqual("sensor_1"))
                .select($("id"), $("ts"), $("vc"));

        //将结果表转换为 流（只有添加操作，使用append流）

        DataStream<Row> rowDataStream = tableEnv.toAppendStream(resultTable, Row.class);

        rowDataStream.print();*/

        //todo 聚合查询

        Table result = table.where($("vc").isGreaterOrEqual(30))
                .groupBy($("id"))
                .aggregate($("vc").sum().as("vc_sum"))
                .select($("id"),$("vc_sum"));


        DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(result, Row.class);

        retractStream.print();


        env.execute();


    }
}
