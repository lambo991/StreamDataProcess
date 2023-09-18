package com.atguigu.flink.dataStreamAPI.windows;

import com.atguigu.flink.dataStreamAPI.function.ClickSource;
import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.pojo.WordCount;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class nonKeyedWindow {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> ds = env.addSource(new ClickSource());
        ds.print("data");
//                ds.assignTimestampsAndWatermarks(
//                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
//                                .withTimestampAssigner(
//                                        (event, ts) -> event.getTs()
//                                )
//                ).countWindowAll(5L)
//                .max("ts")
//                .print("window");
        ds.map( event -> new WordCount(event.getUrl(),1))

                .windowAll(
                        TumblingProcessingTimeWindows.of(Time.seconds(10L))
//                        TumblingEventTimeWindows.of(Time.seconds(10))

                ).sum("count")
                .print("window");
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


}
