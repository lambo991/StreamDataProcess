package com.atguigu.flink.dataStreamAPI.source;

import com.atguigu.flink.pojo.WordCount;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class fileConnectorDemo {
    public static void main(String[] args) {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //读取文件使用FileSource，ReadTextFile已经过时
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(
                new TextLineInputFormat(),
                new Path("input/word.txt")
        ).build();

//     新   env.fromSource(Source);
//     旧   env.addSource(SourceFunction);
        DataStreamSource<String> ds = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "filesource");

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
                .keyBy( WordCount::getWord)
                .sum("count")
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
