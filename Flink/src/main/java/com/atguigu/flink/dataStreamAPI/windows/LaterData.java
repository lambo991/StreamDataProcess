package com.atguigu.flink.dataStreamAPI.windows;

import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.pojo.WordCount;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class LaterData {
    public static void main(String[] args) {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);


        SingleOutputStreamOperator<Event> ds = env.socketTextStream("hadoop102",8888)
                .map(
                        line -> {
                            String[] fields = line.split(",");
                            return new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim()));

                        }

                )
                .assignTimestampsAndWatermarks(
                        //水位线推迟2秒
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner((event, ts) -> event.getTs())
                );

        ds.print("data" );

        OutputTag<WordCount> late = new OutputTag<>("late", Types.POJO(WordCount.class));
        SingleOutputStreamOperator<WordCount> windowsSO = ds.map(event -> new WordCount(event.getUser(), 1))
                .keyBy(WordCount::getWord)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                //窗口延迟5秒关闭
                .allowedLateness(Time.seconds(5))
                .sideOutputLateData(late)
                .sum("count");
        windowsSO.print("window");

        //捕获侧输出流
        windowsSO.getSideOutput(late).print("late");

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
