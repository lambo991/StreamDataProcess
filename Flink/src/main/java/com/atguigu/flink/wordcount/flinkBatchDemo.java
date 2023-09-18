package com.atguigu.flink.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class flinkBatchDemo {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> ds = env.readTextFile("input/word.txt");

        FlatMapOperator<String, Tuple2<String, Integer>> flatMap = ds.flatMap(
                new FlatMapFunction<String, Tuple2<String, Integer>>() {

                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String[] words = line.split(" ");
                        for (String word : words) {
                            collector.collect(Tuple2.of(word, 1));
                        }


                    }
                }
        );
        UnsortedGrouping<Tuple2<String, Integer>> group = flatMap.groupBy(0);

        AggregateOperator<Tuple2<String, Integer>> sum = group.sum(1);

        sum.print();

    }
}
