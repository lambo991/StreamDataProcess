package com.atguigu.flink.dataStreamAPI.state;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class BroadcastState {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        //开启检查点后，无线重启
        env.enableCheckpointing(2000);

        DataStreamSource<String> ds = env.socketTextStream("hadoop102", 8888);

        DataStreamSource<String> confDS = env.socketTextStream("hadoop102", 9999);
        MapStateDescriptor<String, String> mapStateDescriptor =
                new MapStateDescriptor<>("mapState", Types.STRING, Types.STRING);
        BroadcastStream<String> broadcastDs = confDS.broadcast(mapStateDescriptor);

        ds
                .connect(broadcastDs)
                .process(
                        new BroadcastProcessFunction<String,String,String>(){

                            @Override
                            public void processElement(String value, BroadcastProcessFunction<String, String, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {

                                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                                String flag = broadcastState.get("flag");
                                if("1".equals(flag)) {
                                    System.out.println("1号逻辑");
                                } else if ("2".equals(flag)) {
                                    System.out.println("2号逻辑");
                                }
                                else {
                                    System.out.println("执行默认逻辑");
                                }

                                out.collect(value.toUpperCase());
                            }

                            @Override
                            public void processBroadcastElement(String value, BroadcastProcessFunction<String, String, String>.Context ctx, Collector<String> out) throws Exception {
                                //将广播流中的数据，存到广播状态中
                                org.apache.flink.api.common.state.BroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

                                broadcastState.put("flag",value);
                            }
                        }

                ).print()
                ;
        env.execute();



    }
}
