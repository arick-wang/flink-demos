package com.znv.kafka;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @Author: wr
 * @Date: 2020/1/16 17:27
 * @Description:
 */
public class FlinkKafkaConsumerService {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkKafkaConsumer flinkKafkaConsumer = new FlinkKafkaConsumer("bap-etl-door-event", new SimpleStringSchema(), getProperties());
        flinkKafkaConsumer.setStartFromEarliest();
        DataStream ds = env.addSource(flinkKafkaConsumer);
        ds.print();
        env.execute("kafka flink test");
    }


    public static Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "face.dct-znv.com:9092");
        properties.setProperty("group.id", "testflinkgroup1");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return properties;
    }
}
