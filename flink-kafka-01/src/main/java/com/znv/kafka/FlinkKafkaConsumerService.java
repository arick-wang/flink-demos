package com.znv.kafka;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.IOException;
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
        ds.addSink(new JsonPrintSink());
        env.execute("kafka flink test");
    }

    static class JsonPrintSink extends RichSinkFunction<String> {

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("sink open");
        }

        @Override
        public void close() throws Exception {
            System.out.println("sink close");
        }

        @Override
        public void invoke(String value, Context context) {
            System.out.println("one time");
            System.out.println(value);
        }
    }

    public static Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "face.dct-znv.com:9092");
        properties.setProperty("group.id", "testflinkgroup12");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return properties;
    }
}
