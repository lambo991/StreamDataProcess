package com.atguigu.flink.dataStreamAPI.state;

import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class keyedByListState {
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

                            private ListState<Integer> listState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                ListStateDescriptor<Integer> listStateDescriptor = new ListStateDescriptor<>("listState", Types.INT);
                                listState = getRuntimeContext().getListState(listStateDescriptor);

                            }

                            @Override
                            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {

                                listState.add(value.getVc());

                                Iterable<Integer> integers = listState.get();
                                List<Integer> vcList = new ArrayList<>();
                                for (Integer integer : integers) {
                                    vcList.add(integer);
                                }
                                vcList.sort(
                                        new Comparator<Integer>() {
                                            @Override
                                            public int compare(Integer o1, Integer o2) {
                                                return Integer.compare(o2,o1);
                                            }
                                        }
                                );
                                if (vcList.size() > 3) {
                                    vcList.remove(3);
                                }
                                //更新状态
                                listState.update(vcList);
                                out.collect(value.getId()  + "传感器最高的三个水位值 "+ vcList);

                            }
                        }
                ).print()
                ;
        
        env.execute();

    }
}
