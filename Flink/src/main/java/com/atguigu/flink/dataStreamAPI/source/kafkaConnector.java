package com.atguigu.flink.dataStreamAPI.source;

import com.alibaba.fastjson.JSON;
import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.pojo.WordCount;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 *
 * TODO
 *  1.
 *  kafka如何消费数据
 *  基于拉取的方式，从定义的topic中的每个partition中消费数据
 *  2.
 *  消费的原则：
 *  以消费者组的名义消费数据
 *  一个消费者能消费一个主题中的多个分区的数据
 *  一个主题中的一个分区的数据只能被一个消费者组中的说个消费者消费
 *  3.
 *  消费者offset如何维护
 *      维护的位置： 0.9之前存在zk中，之后存在Kafka的consumer_offsets主题中
 *      offset 的组成：groupId：topic：partition：offset
 *  4.
 *  消费者代码怎么写
 *  Properties properties = new Properties();
 *  props.put(...);
 *  KafkaConsumer consumer = new KafkaConsumer(properties);
 *  consumer.subscribe(topics); 订阅
 *  ConsumerRecord consumerRecord = consumer.poll();
 *  consumerRecord以 KV的方式
 *  5.
 *  消费者的配置
 *      消费者组：group.id
 *      集群地址： bootstrap.servers
 *      key的反序列化器：key.deserializer
 *      value的反序列化器：value.deserializer
 *      事务的隔离级别：isolation.level[read_committed,read_uncommitted]
 *      offset自动提交： enable.auto.commit
 *      offset自动提交间隔：auto.commit.interval.ms
 *      offset重置策略: auto.offset.reset
 *          重置的情况：
 *              1.新的消费者组（之前没有消费过，kafka中没有对应的offset）
 *              2.旧的消费者组，但是要消费的offset在kafka中已经不存在，因为超过数据的存储周期，数据被删除
 *          重置的策略：
 *              1.earliest:
 *              2.latest:
 */


public class kafkaConnector {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//
//        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
//                .setBootstrapServers("hadoop102:9092,hadoop103:9092")
//                .setGroupId("flink")
//                .setTopics("test")
////                设置key反序列化器
////                .setDeserializer(KafkaRecordDeserializationSchema.of())
//                //设置value反序列化器
//                .setValueOnlyDeserializer(new SimpleStringSchema())
//                //从哪里消费
//                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
//                .build();
//        DataStreamSource<String> ds = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");
//        ds.print();

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setGroupId("flink")
                .setTopics("tms_ods")

                .build();

        DataStreamSource<String> ds = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");

        ds.map(
                        new MapFunction<String, Event>() {
                            @Override
                            public Event map(String line) throws Exception {
                                return JSON.parseObject(line, Event.class);
                            }
                        }
                )
                        .map(event -> new WordCount(event.getUrl(),1))
                                .keyBy(WordCount::getWord)
                                        .reduce(
                                                new ReduceFunction<WordCount>() {
                                                    @Override
                                                    public WordCount reduce(WordCount w1, WordCount w2) throws Exception {
                                                        return new WordCount(w1.getWord(),w1.getCount() + w2.getCount());
                                                    }
                                                }
                                        )
                                                .print();


        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
