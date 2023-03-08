package com.atguigu.testfunction;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.util.Map;
import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.uuid;

/**
 * @author MengX
 * @create 2023/3/7 20:33:32
 */
public class Test_TableApiConnect {

    private static Schema schema;

    public static void main(String[] args) throws Exception {

        //获取流环境

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        schema = new Schema();

        schema
                .field("id", DataTypes.STRING())
                .field("ts",DataTypes.BIGINT())
                .field("vc",DataTypes.INT());

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.connect( new FileSystem().path("input/sensor_sql.txt"))
                .withSchema(schema)
                .withFormat(new Csv().fieldDelimiter(',').lineDelimiter("\n"))
                .createTemporaryTable("sersor");

        Table sersor = tableEnv.from("sersor");

        Table select = sersor
                .where($("id").isEqual("sensor_1"))
                .groupBy($("id"))
                .select($("id"), $("vc").sum().as("vc_sum"));

        tableEnv.toRetractStream(select, Row.class).print();

        env.execute();
    }
}
