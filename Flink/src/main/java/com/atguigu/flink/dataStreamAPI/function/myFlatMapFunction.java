package com.atguigu.flink.dataStreamAPI.function;

import com.atguigu.flink.pojo.WordCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class myFlatMapFunction implements FlatMapFunction<String, WordCount> {

    private String seperator;

    public myFlatMapFunction(String seperator) {
        this.seperator = seperator;
    }

    @Override
    public void flatMap(String value, Collector<WordCount> collector) throws Exception {
        String[] words = value.split(seperator);
        for (String word : words) {
            collector.collect(new WordCount(word,1));
        }
    }
}
