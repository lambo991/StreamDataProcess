package com.atguigu.flink.dataStreamAPI.exec;

import com.alibaba.fastjson.JSON;
import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.pojo.WordCount;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.time.Duration;

public class FlinkKafkaDemo2 {

    /*
    1.
    时间语义
        事件时间
        处理时间
        摄入时间

     2.


     可以每条数据都生成水位线
     也可以间隔时间生成水位线



     3.
     滚动窗口
     滑动窗口
     会话窗口
     全局窗口
      4.
      reduceFunction 输入输出类型要一致
      AggregateFunction 输入输出类型可以不一致
      5.
      1.延迟水位线生成时间
        窗口延迟关闭
        对长时间没有到达的异常数据通过侧输出流进行单独处理
     */
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setGroupId("kafkaSource")
                .setTopics("test3")
                .build();
        SingleOutputStreamOperator<Event> ds = env.fromSource(kafkaSource,WatermarkStrategy.noWatermarks(),"kafkaSource")
                .map(line -> {
                    String[] words = line.split(",");
                    return new Event(words[0].trim(), words[1].trim(), Long.parseLong(words[2].trim()));
                });
        ds.print("input");
        SingleOutputStreamOperator<WordCount> countDS = ds.assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((event, ts) -> event.getTs())
                ).map(event -> new WordCount(event.getUser(), 1))
                .keyBy(WordCount::getWord)
                .window(
                        TumblingEventTimeWindows.of(Time.seconds(10))
                )
                .sum("count");
        countDS.print("count");


        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema
                                .builder()
                                .setTopic("test4")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "600000")
                .build();


        countDS.map(JSON::toJSONString).sinkTo(kafkaSink);

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
