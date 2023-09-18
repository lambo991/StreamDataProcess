package com.atguigu.flink.dataStreamAPI.state;

import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class keyedByMapState {
    public static void main(String[] args) throws Exception {
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

                            private MapState<Integer,String> mapState;

                            @Override
                            public void open(Configuration parameters) throws Exception {

                                MapStateDescriptor<Integer,String> mapStateDescriptor = new MapStateDescriptor<>("mapState",  Types.INT,Types.STRING);
                                mapState = getRuntimeContext().getMapState(mapStateDescriptor);
                            }

                            @Override
                            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {

                                mapState.put(value.getVc(),null);

                                out.collect(value.getId() + "当前传感器输出的水位线为：" + mapState.keys().toString());


                            }
                        }
                ).print()
        ;

        env.execute();

    }
}
