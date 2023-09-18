package com.atguigu.flink.dataStreamAPI.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class OperatorState {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        //开启检查点后，无线重启
        env.enableCheckpointing(2000);
        //重启尝试3次，每次间隔2秒
//        env.setRestartStrategy( RestartStrategies.fixedDelayRestart(3, Time.seconds(2)));
        DataStreamSource<String> ds = env.socketTextStream("hadoop102", 8888);

        ds.map(
                new MyMapFunction()
        ).addSink(
                new SinkFunction<String>() {
                    @Override
                    public void invoke(String value, Context context) throws Exception {
                        if (value.contains("x")) {
                            throw new RuntimeException("异常-------------------");
                        }
                        System.out.println("sink：" + value);
                    }
                }
        );


        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public static class MyMapFunction implements MapFunction<String,String>, CheckpointedFunction {

        //通过初始化方法创建 状态集合
        private ListState<String> listState;


        @Override
        public String map(String s) throws Exception {
            listState.add(s);
            return listState.get().toString();
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            System.out.println("MyMapFunction.snapshotState");
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<String> listStateDescriptor = new ListStateDescriptor<>("listState", Types.STRING);
            // UnionListState
            listState = context.getOperatorStateStore().getUnionListState(listStateDescriptor);
/*
             ListState
            listState = context.getOperatorStateStore().getListState( listStateDescriptor);
*/
        }
    }
}
