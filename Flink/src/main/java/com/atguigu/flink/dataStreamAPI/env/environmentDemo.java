package com.atguigu.flink.dataStreamAPI.env;

import com.atguigu.flink.pojo.WordCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class environmentDemo {

    public static void main(String[] args) {



//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        http://hadoop104:35287/
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("39.100.144.185", 46401, "input/flink.jar");
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStreamSource<String> ds = env.socketTextStream("hadoop102", 8888);

        ds.flatMap(
                new FlatMapFunction<String, WordCount>() {
                    @Override
                    public void flatMap(String line, Collector<WordCount> collector) throws Exception {

                        String[] words = line.split(" ");
                        for (String word : words) {
                            collector.collect(new WordCount(word,1));
                        }

                    }
                }
        )
                .keyBy(WordCount::getWord)
                .sum("count")
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
