package com.atguigu.flink.dataStreamAPI.timeAndWaterMark;

import com.atguigu.flink.dataStreamAPI.function.ClickSource;
import com.atguigu.flink.pojo.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class waterMarkDemo {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        //设置水位线生成间隔时间
        env.getConfig().setAutoWatermarkInterval(5000);

        DataStreamSource<Event> ds = env.addSource(new ClickSource());

        SingleOutputStreamOperator<Event> eventSO = ds.assignTimestampsAndWatermarks(
//                有序流生成水位线
//                WatermarkStrategy.<Event>forMonotonousTimestamps()
//                        .withTimestampAssigner(
//                        new SerializableTimestampAssigner<Event>() {
//                            @Override
//                            public long extractTimestamp(Event event, long l) {
//                                return event.getTs();
//
//                            }
//                        }
//                )
//                无序流生成水位线
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(
                                (event, l) -> event.getTs()
                        )
        );

    }
}
