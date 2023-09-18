package com.atguigu.flink.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class flinkBoundedStreamDemo {
    public static void main(String[] args) {
        //1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度，默认为CPU数
//        env.setParallelism(1);

        //2.读取数据
        DataStreamSource<String> ds = env.readTextFile("input/word.txt");

        //3.转换处理
        SingleOutputStreamOperator<Tuple2<String, Integer>> flattedMap = ds.flatMap(
                new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String[] words = line.split(" ");
                        for (String word : words) {

                            collector.collect(Tuple2.of(word, 1));
                        }
                    }


                }
        );


        //4.输出结果
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = flattedMap.keyBy(
                new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                }
        );
        keyedStream
                .sum(1)
                .print();
        //5.启动执行
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }

}
