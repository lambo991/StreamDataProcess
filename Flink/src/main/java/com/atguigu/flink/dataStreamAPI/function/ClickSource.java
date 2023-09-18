package com.atguigu.flink.dataStreamAPI.function;

import com.atguigu.flink.pojo.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class ClickSource implements SourceFunction<Event> {


    String[] users = {"zhangsan","lisi","wangwu","chenliu","xieqi"};

    String[] urls = {"/home","/cart","/pay","/detail","/list"};

    Random random = new Random();

    private Boolean isRunning = true;
    /**
     * 模拟生成数据的方法
     * @param sourceContext
     * @throws Exception
     */
    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        //持续生成数据
        while (isRunning) {
            //随机数据
            String user = users[random.nextInt(users.length)];
            String url  = urls[random.nextInt(urls.length)];
            Long ts = System.currentTimeMillis();
            //生成event
            Event event = new Event(user, url, ts);
            //发送数据
            sourceContext.collect(event);
            //每秒生成一条数据
            TimeUnit.SECONDS.sleep(1);
        }
    }
    //取消生成
    @Override
    public void cancel() {

    }
}
