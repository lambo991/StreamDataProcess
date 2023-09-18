package com.atguigu.flink.dataStreamAPI.sink;

import com.atguigu.flink.dataStreamAPI.function.ClickSource;
import com.atguigu.flink.pojo.Event;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MySQLSinkDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointStorage("file:\\D:\\Files\\checkpoint");
        env.enableCheckpointing(200);

        DataStreamSource<Event> ds = env.addSource(new ClickSource());

        SinkFunction<Event> MySQLSinkFunction = JdbcSink.sink(
//                "insert into clicks(user,url,ts) values( ?,?,?) ",
//                "replace into clicks(user,url,ts) values( ?,?,?) ",
                "insert into clicks(user,url,ts) values( ?,?,?)  on duplicate key update url = VALUE(url) and ts = VALUE(ts)",
                new JdbcStatementBuilder<Event>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, Event event) throws SQLException {
                        preparedStatement.setString(1, event.getUser());
                        preparedStatement.setString(2, event.getUrl());
                        preparedStatement.setLong(3, event.getTs());
                    }
                }
                ,
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://39.100.144.185:3306/test")
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()
        );
        ds.addSink(MySQLSinkFunction);

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
