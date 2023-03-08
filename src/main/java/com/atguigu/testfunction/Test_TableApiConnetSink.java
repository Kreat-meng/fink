package com.atguigu.testfunction;

import bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;


import static org.apache.flink.table.api.Expressions.$;

/**
 * @author MengX
 * @create 2023/3/7 21:22:31
 */
public class Test_TableApiConnetSink {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<WaterSensor> streamSource = env.fromElements(

                new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60)
        );

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Table table = tableEnv.fromDataStream(streamSource);

        Table select = table
                .where($("id").isEqual("sensor_1"))
                .select($("id"), $("ts"), $("vc"));

        Schema schema = new Schema();

        schema
                .field("id", DataTypes.STRING())
                .field("ts",DataTypes.BIGINT())
                .field("vc",DataTypes.INT());

        tableEnv
                .connect(new FileSystem().path("output/sensor_sql.txt"))
                .withFormat(new Csv().fieldDelimiter(','))
                .withSchema(schema)
                .createTemporaryTable("sensor");

        select.executeInsert("sensor");


    }
}
