package com.atguigu.flink.dataStreamAPI.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

public class RawState {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //重启尝试3次，每次间隔2秒
        env.setRestartStrategy( RestartStrategies.fixedDelayRestart(3, Time.seconds(2)));
        DataStreamSource<String> ds = env.socketTextStream("hadoop102", 8888);

        ds.map(
                new MyMapFunction()
        ).addSink(
                new SinkFunction<String>() {
                    @Override
                    public void invoke(String value, Context context) throws Exception {
                        if (value.contains("x")) {
                            throw new RuntimeException("异常-------------------");
                        }
                        System.out.println("sink：" + value);
                    }
                }
        );


        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public static class MyMapFunction implements MapFunction<String,String> {

        //原始状态
        private List<String> words = new ArrayList<>();

        //将map维护到一个集合中，集中输出
        @Override
        public String map(String s) throws Exception {
            words.add(s);
            return words.toString();
        }
    }
}
