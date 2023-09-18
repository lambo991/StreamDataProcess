package com.atguigu.flink.dataStreamAPI.windows;

import com.atguigu.flink.pojo.OrderDetailEvent;
import com.atguigu.flink.pojo.OrderEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class IntervalJoin {
    public static void main(String[] args) {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<OrderEvent> orderDs = env.socketTextStream("hadoop102", 8888)
                .map(line -> {
                    String[] words = line.split(",");
                    return new OrderEvent(words[0].trim(), Long.parseLong(words[1].trim()));
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((event, ts) -> event.getTs())
                );
        SingleOutputStreamOperator<OrderDetailEvent> detailDs = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] words = line.split(",");
                    return new OrderDetailEvent(words[0].trim(),words[1].trim(), Long.parseLong(words[2].trim()));
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderDetailEvent>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((event, ts) -> event.getTs())
                );

        //Interval Join

        orderDs.keyBy( OrderEvent::getOrderId).intervalJoin(
                detailDs.keyBy(OrderDetailEvent::getOrderId)
        )
                .between(Time.seconds(-2),Time.seconds(3)) //划定时间界限 ,以第一条流划定界限
//                .upperBoundExclusive() //排除上界
//                .lowerBoundExclusive() //排除下界
                .process(
                        new ProcessJoinFunction<OrderEvent, OrderDetailEvent, String>() {
                            //JOIN成功会执行该方法
                            @Override
                            public void processElement(OrderEvent orderEvent, OrderDetailEvent orderDetailEvent, ProcessJoinFunction<OrderEvent, OrderDetailEvent, String>.Context context, Collector<String> collector) throws Exception {
                                collector.collect(orderEvent + " --- " + orderDetailEvent);
                            }
                        }
                ).print("Interval Join");

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
