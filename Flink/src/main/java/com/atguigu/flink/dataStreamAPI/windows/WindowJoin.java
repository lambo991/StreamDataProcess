package com.atguigu.flink.dataStreamAPI.windows;

import com.atguigu.flink.pojo.OrderDetailEvent;
import com.atguigu.flink.pojo.OrderEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class WindowJoin {
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
//        windowJoin

        orderDs.join(detailDs)
                .where(OrderEvent::getOrderId) //第一条流需要join的key
                .equalTo(OrderDetailEvent::getOrderId) //第二条流需要Join的key
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(
                        //处理成一条数据调用JoinFunction
                        new JoinFunction<OrderEvent, OrderDetailEvent, Object>() {
                            //Join成功的数据，会执行该方法
                            @Override
                            public Object join(OrderEvent orderEvent, OrderDetailEvent orderDetailEvent) throws Exception {
                                return orderEvent + " --- " + orderDetailEvent;
                            }
                        }
                        //处理成多条数据调用FlatFunciton
//                        new FlatJoinFunction<OrderEvent, OrderDetailEvent, Object>() {
//                        }
                ).print("windowJoin");
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
