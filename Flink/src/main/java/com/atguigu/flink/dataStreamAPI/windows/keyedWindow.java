package com.atguigu.flink.dataStreamAPI.windows;

import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.pojo.WordCount;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;

import java.time.Duration;

public class keyedWindow {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> ds = env.socketTextStream("hadoop102", 8888)
                .map(
                        line -> {
                            String[] words = line.split(",");
                            return new Event(words[0].trim(), words[1].trim(), Long.valueOf(words[2].trim()));

                        }
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((event, ts) -> event.getTs())
                );
        ds.print("data");

//                ds.map( event -> new WordCount(event.getUser(),1))
//                .keyBy(WordCount::getWord)
//                //keyedWindow 计数窗口
//                .countWindow(5L)
////                .countWindow(5L,3L)
//                .sum("count")
//                .print("window");

        ds.map(event -> new WordCount(event.getUser(),1))
                .keyBy(WordCount::getWord)
                .window(
                          //处理时间语义滚动窗口
//                        TumblingProcessingTimeWindows.of(Time.seconds(5))
                        //事件时间语义滚动窗口 通过事件时间来滚动窗口,窗口的滚动与key无关，窗口的计算与key有关
//                        TumblingEventTimeWindows.of(Time.seconds(3))
                        //处理时间语义滑动窗口
//                            SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(5))
                        //时间事件语义滑动窗口
//                        SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5))
                        //处理时间语义会话窗口
//                        ProcessingTimeSessionWindows.withGap(Time.seconds(5))
                        //事件时间语义会话窗口
//                        EventTimeSessionWindows.withGap(Time.seconds(5))'
                        //全局窗口
                        GlobalWindows.create()
                ).sum("count")
                .print("window");

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
