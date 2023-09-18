package com.atguigu.flink.dataStreamAPI.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.UUID;

public class dataGenConnector {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // TODO Flink 自带的 DataGen  数据生成器
        DataGeneratorSource<String> dateGeneratorSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, String>() {

                    @Override
                    public String map(Long value) throws Exception {
                        return UUID.randomUUID().toString() + "--" + value;
                    }
                },
                1000,
                // TODO 可控制频率,每秒生成1条数据
                RateLimiterStrategy.perSecond(1),
                Types.STRING
        );

        DataStreamSource<String> ds = env.fromSource(dateGeneratorSource, WatermarkStrategy.noWatermarks(), "dgSource");

        ds.print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
