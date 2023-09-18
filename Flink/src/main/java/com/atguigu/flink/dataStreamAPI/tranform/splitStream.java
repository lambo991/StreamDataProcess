package com.atguigu.flink.dataStreamAPI.tranform;

import com.atguigu.flink.dataStreamAPI.function.ClickSource;
import com.atguigu.flink.pojo.Event;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class splitStream {
    public static void main(String[] args) {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> ds = env.addSource(new ClickSource());
        //简单分流
        SingleOutputStreamOperator<Event> zlFilter = ds.filter(
                event -> "zhangsan".equals(event.getUser()) || "lisi".equals(event.getUser())

        );

        SingleOutputStreamOperator<Event> wcFilter = ds.filter(
                event -> "chenliu".equals(event.getUser()) || "wangwu".equals(event.getUser())

        );
//        zlFilter.print("zlFilter" );
//        wcFilter.print("wcFilter");

        //侧输出流,使用process 的Output
        //分流标签
        OutputTag zlOutputTag = new OutputTag("zlOutputTag", Types.POJO(Event.class));
        OutputTag wcOutputTag = new OutputTag("wcOutputTag", Types.POJO(Event.class));
        SingleOutputStreamOperator<Event> processDs = ds.process(
                new ProcessFunction<Event, Event>() {
                    @Override
                    public void processElement(Event event, ProcessFunction<Event, Event>.Context context, Collector<Event> collector) throws Exception {
                        if ("zhangsan".equals(event.getUser()) || "lisi".equals(event.getUser())) {
                            context.output(zlOutputTag, event);
                        } else if ("chenliu".equals(event.getUser()) || "wangwu".equals(event.getUser())) {
                            context.output(wcOutputTag, event);
                        } else collector.collect(event);

                    }
                }
        );
        processDs.print("other");
        //打印测流
        processDs.getSideOutput(zlOutputTag).print("zlOutputTag");
        processDs.getSideOutput(wcOutputTag).print("wcOutputTag");



        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
