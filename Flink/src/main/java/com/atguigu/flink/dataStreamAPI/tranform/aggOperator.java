package com.atguigu.flink.dataStreamAPI.tranform;

import com.atguigu.flink.dataStreamAPI.function.ClickSource;
import com.atguigu.flink.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class aggOperator {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> ds = env.addSource(new ClickSource());

        ds.print("origin");
//        ds.map( event -> new WordCount(event.getUser(),1))
//                .keyBy(WordCount::getWord)
//                .sum("count")
//                .print("sum");

        ds.keyBy(Event::getUser)
                .min("ts")
                .print("min");
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
