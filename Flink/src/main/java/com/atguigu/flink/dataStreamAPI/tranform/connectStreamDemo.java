package com.atguigu.flink.dataStreamAPI.tranform;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

public class connectStreamDemo {

    public static void main(String[] args) {



        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
        DataStreamSource<Integer> ds1 = env.fromCollection(list);
        DataStreamSource<String> ds2 = env.fromElements("a", "b", "c", "d", "e");

        ConnectedStreams<Integer, String> connectedStreams = ds1.connect(ds2);
        //处理
        connectedStreams.process(
                new CoProcessFunction<Integer, String, String>() {
                    @Override
                    public void processElement1(Integer integer, CoProcessFunction<Integer, String, String>.Context context, Collector<String> collector) throws Exception {
                        collector.collect(integer + "");
                    }

                    @Override
                    public void processElement2(String s, CoProcessFunction<Integer, String, String>.Context context, Collector<String> collector) throws Exception {
                        String upperCase = s.toUpperCase();
                        collector.collect(upperCase);
                    }
                }
        ).print();



        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
