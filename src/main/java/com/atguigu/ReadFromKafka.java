package com.atguigu;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @author MengX
 * @create 2023/2/23 21:23:30
 */
public class ReadFromKafka {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO flink1.13 及以后官方推荐的写法
        /*KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092")
                .setGroupId("0926")
                .setTopics("flink")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaSource1 = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");*/

        // TODO flink1.13 以前的老写法


        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"0926");

        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        DataStreamSource<String> kafkaSource = env.addSource(new FlinkKafkaConsumer<String>("flink", new SimpleStringSchema(), properties));

        kafkaSource.print();

        env.execute();
    }
}
