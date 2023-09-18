package com.atguigu.flink.dataStreamAPI.tranform;

import com.alibaba.fastjson.JSON;
import com.atguigu.flink.dataStreamAPI.function.ClickSource;
import com.atguigu.flink.pojo.Event;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class processOperator {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> ds = env.addSource(new ClickSource());
        ds.process(

                new ProcessFunction<Event, String>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        System.out.println("open");
                        super.open(parameters);

                    }

                    @Override
                    public void processElement(Event event, ProcessFunction<Event, String>.Context context, Collector<String> collector) throws Exception {
                        collector.collect(JSON.toJSONString(event));
                        //定时器编程
//                        context.timerService();
                        //测流
//                        context.output(new OutputTag("sc", Types.POJO(Event.class)),event);
                    }
                }
        ).print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
