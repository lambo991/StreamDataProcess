package com.atguigu.flink.dataStreamAPI.process;

import com.alibaba.fastjson.JSON;
import com.atguigu.flink.pojo.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class timerService {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> ds = env.socketTextStream("hadoop102", 8888).map(
                        line -> {
                            String[] words = line.split(",");
                            return new Event(words[0].trim(), words[1].trim(), Long.parseLong(words[2].trim()));
                        }
                        )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((event, l) -> event.getTs())
                );
        ds.print("data");
        //处理时间定时器
//        ds
//                .keyBy(Event::getUser)
//                .process(
//                        new KeyedProcessFunction<String, Event, String>() {
//                            @Override
//                            public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
//
//
//                                TimerService timerService = ctx.timerService();
//                                long currentProcessingTime = timerService.currentProcessingTime();
//                                System.out.println("currentProcessingTime 当前处理时间：" + currentProcessingTime);
//                                long timerTime = currentProcessingTime + 5000L;
//                                timerService.registerProcessingTimeTimer(timerTime);
//                                System.out.println("processElement 注册处理时间定时器：" + timerTime);
//                                out.collect(JSON.toJSONString(value));
//                            }
//                            //定义定时器
//                            @Override
//                            public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
//                                System.out.println("onTimer 当前触发的处理时间定时器：" + timestamp);
//                            }
//                        }
//                ).print("time");
        //事件时间定时器
        ds
                .keyBy(Event::getUser)
                .process(
                        new KeyedProcessFunction<String, Event, String>() {
                            @Override
                            public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {

                                TimerService timerService = ctx.timerService();
                                //水位线在数据之后生成，首次水位线为Long最小值
                                long watermark = timerService.currentWatermark();

                                System.out.println("processElement 当前水位线为：" + watermark);

                                long timerTime = value.getTs() + 5000L;
                                System.out.println("processElement 当前定时器时间：" + timerTime);

                                timerService.registerEventTimeTimer(timerTime);
                                out.collect(JSON.toJSONString(value));
                            }

                            @Override
                            public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                                System.out.println("触发定时器时间为：" + timestamp);
                            }
                        }
                ).print("EventTimer");



        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
