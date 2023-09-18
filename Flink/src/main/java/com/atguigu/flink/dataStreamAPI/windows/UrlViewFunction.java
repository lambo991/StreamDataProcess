package com.atguigu.flink.dataStreamAPI.windows;

import com.atguigu.flink.dataStreamAPI.function.ClickSource;
import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.pojo.UrlViewCount;
import com.atguigu.flink.pojo.WordCount;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class UrlViewFunction {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);


        SingleOutputStreamOperator<Event> ds = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((event, ts) -> event.getTs())
                );

        ds.print("data" );

        //统计10秒钟的url浏览量，每5秒钟更新一次
        ds.map(event -> new WordCount(event.getUrl(),1))
                .keyBy(WordCount::getWord)
                .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
                .aggregate(
                        //增量聚合函数负责url点击次数的统计
                        new AggregateFunction<WordCount, UrlViewCount, UrlViewCount>() {
                            @Override
                            public UrlViewCount createAccumulator() {
                                return new UrlViewCount();
                            }

                            @Override
                            public UrlViewCount add(WordCount wordCount, UrlViewCount urlViewCount) {
                                urlViewCount.setCount(urlViewCount.getCount() + 1);
                                return urlViewCount;
                            }

                            @Override
                            public UrlViewCount getResult(UrlViewCount urlViewCount) {
                                return urlViewCount;
                            }

                            @Override
                            public UrlViewCount merge(UrlViewCount urlViewCount, UrlViewCount acc1) {
                                return null;
                            }
                        },
                        //全窗口函数负责窗口信息的补充
                        new ProcessWindowFunction<UrlViewCount, UrlViewCount, String, TimeWindow>() {
                            @Override
                            public void process(String key, ProcessWindowFunction<UrlViewCount, UrlViewCount, String, TimeWindow>.Context context, Iterable<UrlViewCount> iterable, Collector<UrlViewCount> collector) throws Exception {
                                UrlViewCount urlViewCount = iterable.iterator().next();

                                //补充Url
                                urlViewCount.setUrl(key);

                                urlViewCount.setWindowStart(context.window().getStart());
                                urlViewCount.setWindowEnd(context.window().getEnd());

                                collector.collect(urlViewCount);

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
