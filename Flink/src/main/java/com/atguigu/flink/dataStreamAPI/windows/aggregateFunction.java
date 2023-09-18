package com.atguigu.flink.dataStreamAPI.windows;

import com.atguigu.flink.dataStreamAPI.function.ClickSource;
import com.atguigu.flink.pojo.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.HashSet;

public class aggregateFunction {
    public static void main(String[] args) {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //统计每十秒内的UV (独立访客数)
        //从每条数据中提取User，通过set维护起来，
        SingleOutputStreamOperator<Event> ds = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((event, ts) -> event.getTs())
                );
        ds.print("data");

        ds.map(Event::getUser)
                .windowAll(
                        TumblingEventTimeWindows.of(Time.seconds(10))
                )
                .aggregate(
                        new AggregateFunction<String, HashSet<String>, Integer>() {
                            @Override
                            public HashSet<String> createAccumulator() {
                                return new HashSet<>();
                            }

                            @Override
                            public HashSet<String> add(String user, HashSet<String> set) {
                                set.add(user);
                                return set;
                            }
                            //获取结果，只执行一次
                            @Override
                            public Integer getResult(HashSet<String> set) {
                                return set.size();
                            }

                            @Override
                            public HashSet<String> merge(HashSet<String> strings, HashSet<String> acc1) {
                                return null;
                            }
                        }
                ).print("window");


        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
