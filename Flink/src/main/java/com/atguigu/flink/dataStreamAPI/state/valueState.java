package com.atguigu.flink.dataStreamAPI.state;

import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class valueState {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("hadoop102", 8888)
                .map(
                        line -> {
                            String[] words = line.split(",");
                            return new WaterSensor(words[0].trim(), Integer.parseInt(words[1].trim()), Long.parseLong(words[2].trim()));
                        }
                );
        ds.keyBy(WaterSensor::getId)
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {
                            //声明值状态
                            private ValueState<Integer> valueState;

                            //使用open 初始化值状态
                            @Override
                            public void open(Configuration parameters) throws Exception {
                                ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor<>("valueState", Types.INT);
                                valueState = getRuntimeContext().getState(valueStateDescriptor);
                            }

                            @Override
                            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                                Integer lastVc = valueState.value();
                                valueState.update(value.getVc());
                                if(lastVc == null) {
                                    return;
                                }

                                if (Math.abs(lastVc - value.getVc()) > 10) {
                                    out.collect("警报：上次VC" + lastVc + " 本次VC " + value.getVc() + " 差值大于10" );
                                }
                            }
                        }
                ).print();

                ;

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
