package com.atguigu.flink.deployment;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class flinkStreamDemo {

//com.atguigu.flink.deployment.flinkStreamDemo
//    --hostname hadoop102 --port 8888
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        //读取端口数据
        DataStreamSource<String> ds = env.socketTextStream(parameterTool.get("hostname"), parameterTool.getInt("port"));


        ds.flatMap(
                (String line, Collector<Tuple2<String, Integer>> collector) -> {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        collector.collect(Tuple2.of(word, 1));
                    }
                }
                )
                .returns(
                Types.TUPLE(Types.STRING,Types.INT)

                )
                .keyBy(
                Tuple2 -> Tuple2.f0
                )

                .sum(1)
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
