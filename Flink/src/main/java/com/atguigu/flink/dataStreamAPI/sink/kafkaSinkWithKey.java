package com.atguigu.flink.dataStreamAPI.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.flink.dataStreamAPI.function.ClickSource;
import com.atguigu.flink.pojo.Event;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class kafkaSinkWithKey {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> ds = env.addSource(new ClickSource());

        KafkaSink<Event> kafkaSink = KafkaSink.<Event>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setRecordSerializer(
                        new KafkaRecordSerializationSchema<Event>() {
                            @Nullable
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(Event event, KafkaSinkContext context, Long aLong) {
                                byte[] keyBytes = event.getUser().getBytes();
                                byte[] valBytes = JSON.toJSONString(event).getBytes();

                                return new ProducerRecord<>("test1",keyBytes,valBytes);
                            }
                        }
                )
                .build();
        ds.sinkTo(kafkaSink);
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
