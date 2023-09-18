package com.atguigu.flink.dataStreamAPI.tranform;

import com.atguigu.flink.dataStreamAPI.function.ClickSource;
import com.atguigu.flink.pojo.Event;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class UserDefinePartition {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        DataStreamSource<Event> ds = env.addSource(new ClickSource());

        ds.partitionCustom(
                new Partitioner<String>() {
                    @Override
                    public int partition(String line, int i) {
                        if ("zhangsan".trim().equals(line) || "lisi".trim().equals(line)) {
                            return 0;

                        } else if ("wangwu".trim().equals(line) || "chenliu".equals(line)) {
                            return 1;
                        } else return 2;
                    }
                },
                new KeySelector<Event, String>() {
                    @Override
                    public String getKey(Event event) throws Exception {
                        return event.getUser()  ;
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
