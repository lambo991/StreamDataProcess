package com.atguigu.flink.dataStreamAPI.tranform;

import com.atguigu.flink.dataStreamAPI.function.myFlatMapFunction;
import com.atguigu.flink.pojo.WordCount;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class functionImplement {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        FileSource<String> stringFileSource = FileSource.forRecordStreamFormat(
                new TextLineInputFormat(),
                new Path("input/word.txt")
        ).build();
        DataStreamSource<String> ds = env.fromSource(stringFileSource, WatermarkStrategy.noWatermarks(), "fileSource");

        ds.flatMap(
                new myFlatMapFunction(" ")
        )

                //要聚合，必须先keyBy,Flink语法要求聚合操作必须基于keyedStream来完成,主要目的就是将被计算的数据分到多个分区中，通过多并行度的方式处理
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
