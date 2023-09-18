package com.atguigu.flink.dataStreamAPI.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.flink.dataStreamAPI.function.ClickSource;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

public class kafkaSinkDemo {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<String> jsonDs = env.addSource(new ClickSource()).map(JSON::toJSONString);
        jsonDs.print();
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic("test")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                //精确一次需要配置事务ID
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("fink" + RandomUtils.nextInt(1,100))
                //至少一次
//                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,"600000")
                .build();

        jsonDs.sinkTo(kafkaSink);


        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
