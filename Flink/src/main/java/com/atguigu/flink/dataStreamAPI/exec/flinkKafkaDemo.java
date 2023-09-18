package com.atguigu.flink.dataStreamAPI.exec;


import com.alibaba.fastjson.JSON;
import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.pojo.WordCount;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class flinkKafkaDemo {

    public static void main(String[] args) {
        /*
        1.    map ,
              filter,
              flatmap ,
              reduce
         2. 1.有生命周期
                open() close()
            2.实现逻辑方法

            3.侧输出流
                output

          3.分流
            使用filter

            使用output
            合流
            类型相同使用
            union
            类型不同使用
            connect
          4.使用process方法 对数据进行过滤
            使用 output 打侧输出标签
            最后用getSideOutput 获取相对应的侧输出流
         */

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);




        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setGroupId("flink")
                .setTopics("test1")
                .setValueOnlyDeserializer(new SimpleStringSchema())

                .build();

        DataStreamSource<String> kfkDs = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");


        SingleOutputStreamOperator<WordCount> wordCountSingleOutputStreamOperator = kfkDs.map(
                        line -> JSON.parseObject(line, Event.class)
                )
                .flatMap(
                        new FlatMapFunction<Event, WordCount>() {

                            @Override
                            public void flatMap(Event event, Collector<WordCount> collector) throws Exception {
                                String url = event.getUrl();
                                collector.collect(new WordCount(url, 1));
                            }
                        }

                )
                .keyBy(WordCount::getWord)
                .sum("count");
        wordCountSingleOutputStreamOperator.print();
        KafkaSink<WordCount> kafkaSink = KafkaSink.<WordCount>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setRecordSerializer(
                        new KafkaRecordSerializationSchema<WordCount>() {
                            @Nullable
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(WordCount wordCount, KafkaSinkContext kafkaSinkContext, Long aLong) {
                                byte[] keyBytes = wordCount.getWord().getBytes();
                                byte[] valBytes = JSON.toJSONString(wordCount).getBytes();
                                return new ProducerRecord<>("test2",keyBytes,valBytes);
                            }
                        }
                )
                .build();

        wordCountSingleOutputStreamOperator.sinkTo(kafkaSink);

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }
}
