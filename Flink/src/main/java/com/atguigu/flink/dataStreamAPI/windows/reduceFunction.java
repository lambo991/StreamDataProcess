package com.atguigu.flink.dataStreamAPI.windows;


import com.atguigu.flink.dataStreamAPI.function.ClickSource;
import com.atguigu.flink.pojo.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

//增量聚合 Reduce
public class reduceFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> ds = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((event, ts) -> event.getTs())
                );
        ds.print("data");
        //每十秒内用户点击次数(PV) PageView
        ds.map(
                event -> 1
        )
                .windowAll(
                        TumblingEventTimeWindows.of(Time.seconds(10))
                )
                .reduce(
                        new ReduceFunction<Integer>() {
                            @Override
                            public Integer reduce(Integer integer, Integer t1) throws Exception {
                                return integer + t1;
                            }
                        }
                ).print("reduce");



    }
}
