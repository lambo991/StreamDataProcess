package com.atguigu.flink.dataStreamAPI.windows;

import com.atguigu.flink.dataStreamAPI.function.ClickSource;
import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.pojo.WordCount;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.HashSet;

public class PvUvDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);


        SingleOutputStreamOperator<Event> ds = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((event, ts) -> event.getTs())
                );

        ds.print("data" );

        ds.map( event -> new WordCount(event.getUser(), 1))
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(
                        new AggregateFunction<WordCount, Tuple2<HashSet<String>,Integer>, Double>() {

                            @Override
                            public Tuple2<HashSet<String>, Integer> createAccumulator() {
                                return Tuple2.of(new HashSet<>(),0);
                            }

                            @Override
                            public Tuple2<HashSet<String>, Integer> add(WordCount wordCount, Tuple2<HashSet<String>, Integer> hashSetIntegerTuple2) {
                                hashSetIntegerTuple2.f0.add(wordCount.getWord());
                                return Tuple2.of(hashSetIntegerTuple2.f0,hashSetIntegerTuple2.f1 + 1);
                            }

                            @Override
                            public Double getResult(Tuple2<HashSet<String>, Integer> hashSetIntegerTuple2) {
                                return (double) hashSetIntegerTuple2.f0.size() / hashSetIntegerTuple2.f1;
                            }

                            @Override
                            public Tuple2<HashSet<String>, Integer> merge(Tuple2<HashSet<String>, Integer> hashSetIntegerTuple2, Tuple2<HashSet<String>, Integer> acc1) {
                                return null;
                            }
                        }
                ).print("PvUv");


        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
