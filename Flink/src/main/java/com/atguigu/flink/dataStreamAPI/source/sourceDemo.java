package com.atguigu.flink.dataStreamAPI.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

public class sourceDemo {
    public static void main(String[] args) {

        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> listDS = env.fromCollection(list);



        listDS.print("listDS");
        DataStreamSource<String> elementsDS = env.fromElements("a", "b", "c", "d", "e");
        elementsDS.print("elementsDS");

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
