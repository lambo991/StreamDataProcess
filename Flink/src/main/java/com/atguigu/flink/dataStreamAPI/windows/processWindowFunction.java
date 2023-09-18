package com.atguigu.flink.dataStreamAPI.windows;

import com.atguigu.flink.dataStreamAPI.function.ClickSource;
import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.pojo.WordCount;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class processWindowFunction {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);


        SingleOutputStreamOperator<Event> ds = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((event, ts) -> event.getTs())
                );

        ds.print("data" );
        //每十秒内用户点击次数，

        ds.map(event -> new WordCount(event.getUser(),1))
                .keyBy(WordCount::getWord)
                .window(
                        TumblingEventTimeWindows.of(Time.seconds(10))
                )
                .process(
                        new ProcessWindowFunction<WordCount, String, String, TimeWindow>() {

                            @Override
                            public void process(String key, ProcessWindowFunction<WordCount, String, String, TimeWindow>.Context context, Iterable<WordCount> iterable, Collector<String> collector) throws Exception {

                                long count = 0L;
                                for (WordCount wordCount : iterable) {
                                    count++;

                                }

                                long start = context.window().getStart();
                                long end = context.window().getEnd();

                                collector.collect("窗口 [ " + start + " - " + end + " ) ,  用户 " + key + " 的点击次数为： " + count  );
                            }
                        }
                ).print("key");
                //如果使用的是WindowFunction 需要调用 apply
//                .apply()

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
