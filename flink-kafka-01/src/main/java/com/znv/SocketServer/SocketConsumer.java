package com.znv.SocketServer;

import com.znv.Application;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @Author: wr
 * @Date: 2020/1/16 16:49
 * @Description:
 */
public class SocketConsumer {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> text = env.socketTextStream("localhost", 18080, "\n");
        DataStream<WordWithCount> word = text
                .flatMap(new FlatMapFunction<String, WordWithCount>() {
                    @Override
                    public void flatMap(String s, Collector<WordWithCount> collector) throws Exception {
                        Arrays.stream(s.split(" ")).
                                forEach(str -> collector.collect(new WordWithCount(str, 1L)));
                    }
                })
                .keyBy("word")
                .timeWindow(Time.seconds(5), Time.seconds(5))
                .reduce((a, b) -> new WordWithCount(a.word, a.count + b.count));
        word.print().setParallelism(1);
        env.execute("socket word count");
    }

    public static class WordWithCount {

        public String word;
        public long count;

        public WordWithCount() {
        }

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return word + " : " + count;
        }
    }
}
