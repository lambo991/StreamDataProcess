package com.atguigu.flink.wordcount;

import com.atguigu.flink.pojo.WordCount;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class testWordCount {
    public static void main(String[] args) {

        //一个框架，分布式的计算引擎，对有界或无界的数据做有状态的计算
        Configuration conf = new Configuration();
        conf.setString("rest.address","localhost");
        conf.setInteger("rest.port",5678);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStreamSource<String> ds = env.socketTextStream("hadoop102", 8888);

        ds.flatMap(
                (String line, Collector<WordCount> wordCount) -> {
                    String[] words = line.split(" ");
                    for (String word : words) {

                        wordCount.collect(new WordCount(word,1));
                    }
                }
        )

                .returns(WordCount.class)
                .keyBy(WordCount::getWord)

                .map( WordCount::getCount)
                .name("map1")
                .startNewChain()
                .map(c -> c)
                .name("map2")
                .disableChaining()
                .map(c -> c)
                .name("map3")
//                .startNewChain()
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
