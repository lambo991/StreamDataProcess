package com.atguigu.flink.dataStreamAPI.timeAndWaterMark;

import com.atguigu.flink.pojo.Event;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class userDefineWaterMark {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        //设置水位线生成间隔时间
        env.getConfig().setAutoWatermarkInterval(5000);

        DataStreamSource<String> hadoop102 = env.socketTextStream("hadoop102", 8888);
        SingleOutputStreamOperator<Event> ds = hadoop102.map(str -> {
            String[] words = str.split(",");
            return new Event(words[0].trim(), words[1].trim(), Long.valueOf(words[2]));

        });
//        DataStreamSource<Event> ds = env.addSource(new ClickSource());

        SingleOutputStreamOperator<Event> StreamOperator = ds.assignTimestampsAndWatermarks(
                //水位线策略
                new WatermarkStrategy<Event>() {
                    private Long maxTs = Long.MIN_VALUE;
                    private long delay = 2000L;

                    @Override
                    public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        //创建水位线生成器对象
                        WatermarkGenerator<Event> watermarkGenerator = new WatermarkGenerator<Event>() {
                            //每条数据水位线
                            @Override
                            public void onEvent(Event event, long l, WatermarkOutput watermarkOutput) {
                                maxTs = Math.max(maxTs, event.getTs());
                                watermarkOutput.emitWatermark(new Watermark(maxTs));
                                System.out.println("每条数据都输出水位线：" + maxTs);
                            }
                            //周期性生成水位线
                            @Override
                            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
                                watermarkOutput.emitWatermark(new Watermark(maxTs - delay));
                                System.out.println("周期性输出水位线：" + (maxTs - delay));

                            }
                        };
                        return watermarkGenerator;
                    }
                    @Override
                    public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                        //创建时间戳提取器对象
                        TimestampAssigner<Event> timestampAssigner = new TimestampAssigner<Event>() {
                            //从数据中提取时间
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.getTs();
                            }


                        };
                        return timestampAssigner;



                    }
                }
        );

        StreamOperator.print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
