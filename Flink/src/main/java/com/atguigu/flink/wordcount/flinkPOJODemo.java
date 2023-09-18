package com.atguigu.flink.wordcount;

import com.atguigu.flink.pojo.WordCount;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class flinkPOJODemo {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStreamSource<String> ds = env.readTextFile("input/word.txt");

        ds.flatMap(
                (String line, Collector<WordCount> wordCount ) -> {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        wordCount.collect(new WordCount(word,1));
                    }
                }
        )
                .returns(WordCount.class)
                .keyBy(
                        WordCount::getWord
                )
                .sum("count")
                .print();
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
