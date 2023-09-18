package com.atguigu.flink.dataStreamAPI.function;

import com.alibaba.fastjson.JSON;
import com.atguigu.flink.pojo.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RichFunction {
    public static void main(String[] args) {


        /**
         *
         * TODO 富函数类
         *  除了函数要完成的具体功能的方法之外，还包含了其他的内如，例如：
         *  1.算子生命周期方法： 可以将一些只需要做一次的功能代码，写到生命周期方法中
         *      open() :算子的每个并行实例创建时调用一次;
         *      close() :算子的每个并行实例销毁时调用一次;
         *  2.可以获取运行时上下文 RuntimeContext，进而获取到更多的内容
         *      1）获取当前作业、当前task相关信息
         *      2）状态编程：
         *          getState();
         *          getListState();
         *          getMapState();
         *          getReducingState();
         *          getAggregatingState();
         *  3.富函数类：
         *      RichMapFunction
         *      RichFlatMapFunction
         *      RichFilterFunction
         *      RichReduceFunction
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//
//        DataStreamSource<Event> ds = env.addSource(new ClickSource());
//
        FileSource<String> stringFileSource = FileSource.forRecordStreamFormat(
                new TextLineInputFormat(),
                new Path("input/file.txt")
        ).build();

        SingleOutputStreamOperator<Event> ds = env.fromSource(stringFileSource, WatermarkStrategy.noWatermarks(), "fileSource")
                .map(
                        line -> {
                            String[] fields = line.split(",");
                            return new Event(fields[0].trim(), fields[1].trim(), Long.parseLong(fields[2].trim()));

                        }
                );
        ds.map(

                new RichMapFunction<Event, String>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        System.out.println("open");
                        super.open(parameters);
                    }

                    @Override
                    public String map(Event event) throws Exception {
                        String eventJsonStr = JSON.toJSONString(event);


                        System.out.println(2 + eventJsonStr);

                        return eventJsonStr;

                    }

                    @Override
                    public void close() throws Exception {
                        System.out.println("close");
                        super.close();
                    }
                }
        ).print("map");



        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
