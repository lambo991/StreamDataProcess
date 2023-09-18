package com.atguigu.flink.dataStreamAPI.tranform;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class unionStreamDemo {
    public static void main(String[] args) {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        FileSource<String> stringFileSource = FileSource.forRecordStreamFormat(
                new TextLineInputFormat(),
                new Path("input/word.txt")
        ).build();

        DataStreamSource<String> ds = env.fromSource(stringFileSource, WatermarkStrategy.noWatermarks(),"fileSource");

        SingleOutputStreamOperator<String> javaStreamOperator = ds.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String line, Collector<String> collector) throws Exception {
                        String[] words = line.split(" ");
                        for (String word : words) {

                            if (word.equals("java")) {
                                collector.collect(word);
                            }
                        }

                    }
                }
        );

        ds.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String line, Collector<String> collector) throws Exception {

                        String[] words = line.split(" ");
                        for (String word : words) {

                            if("flink".equals(word)) {
                                collector.collect(word);
                            }
                        }

                    }
                }
        ).union(javaStreamOperator)
                .print("union");




        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

}
