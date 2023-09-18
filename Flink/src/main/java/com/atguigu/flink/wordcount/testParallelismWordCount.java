package com.atguigu.flink.wordcount;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class testParallelismWordCount {
    public static void main(String[] args) {

        Configuration conf = new Configuration();

        conf.setString("rest.address","localhost");
        conf.setInteger("rest.port",5678);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        env.setParallelism(3);

        DataStreamSource<String> ds = env.socketTextStream("hadoop102",8888);

        ds
                .map(w -> w)
                .name("map1")
//                .rebalance()
//                .shuffle()
//                .global()
                .broadcast()
                .map(w -> w)
                .name("map2")
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
