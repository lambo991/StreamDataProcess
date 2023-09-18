package com.atguigu.flink.dataStreamAPI.sink;

import com.alibaba.fastjson2.JSON;
import com.atguigu.flink.dataStreamAPI.function.ClickSource;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;

public class fileSinkDemo {


    public static void main(String[] args) {


//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("39.100.160.221", 38452, "input/flink.jar");
        env.setParallelism(1);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();


        env.enableCheckpointing(2000);

        SingleOutputStreamOperator<String> JsonDs = env.addSource(new ClickSource()).map(
                JSON::toJSONString
        );

        DataStreamSource<String> StrDs = env.fromElements("a", "b", "c", "d", "e");


        FileSink<String> fileSink = FileSink.<String>forRowFormat(
                        new Path("/tmp/data/"),
                        new SimpleStringEncoder<>()
                )
                //滚动策略
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
//                                .withMaxPartSize(new MemorySize(1024 * 1024 * 10))
                                .withMaxPartSize(MemorySize.parse("10mb"))
                                .withRolloverInterval(Duration.ofSeconds(10))
                                .withInactivityInterval(Duration.ofSeconds(5))

                                .build()
                )
                .withBucketCheckInterval(1000L)
                // 目录滚动策略
                .withBucketAssigner(
                        new DateTimeBucketAssigner<>("yyyy-MM-dd HH-mm")
                )
                .withOutputFileConfig(
                        OutputFileConfig.builder()
                                .withPartPrefix("atguigu-")
                                .withPartSuffix("txt")
                                .build()

                )
                .build();
        //新的
        JsonDs.sinkTo(fileSink);
        //旧的
//        StrDs.addSink(SinkFunction)


        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
