package com.atguigu.flink.dataStreamAPI.sink;

import com.atguigu.flink.pojo.Event;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class MysqlSink extends RichSinkFunction<Event>{

    Connection conn = null;
    PreparedStatement ps = null;
    String url = "jdbc:mysql://39.100.144.185:3306/test?useUnicode=true&characterEncoding=utf-8&useSSL=false";
    String username = "root";
    String password = "123456";

    @Override
    public void open(Configuration parameters) throws Exception {
        conn = DriverManager.getConnection(url, username, password);
        conn.setAutoCommit(false);
    }

    @Override
    public void invoke(Event event, Context context) throws Exception {
        String sql = "insert into clicks(user,url,ts) values( ?,?,?) ";
        ps = conn.prepareStatement(sql);
        ps.setString(1,event.getUser());
        ps.setString(2,event.getUrl());
        ps.setLong(3,event.getTs());
        ps.execute();
        conn.commit();
    }

    @Override
    public void close() throws Exception {
        if (conn != null) {
            conn.close();
        }
        if (ps != null) {
            ps.close();
        }

    }
}
