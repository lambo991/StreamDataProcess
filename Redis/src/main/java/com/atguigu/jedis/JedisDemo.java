package com.atguigu.jedis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.ListPosition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JedisDemo {

    public static void main(String[] args) {
        Jedis jedis = getJedisPoll();
        String ping = jedis.ping();
        System.out.println(ping);
        System.out.println("------------------------");
//        testString(jedis);
//        testList(jedis);
//        testSet(jedis);
//        testZSet(jedis);
        testHash(jedis);
        jedis.close();
    }

    private static String host = "hadoop104";

    private static int port = 6379;

    private static JedisPool jedisPool;

//    public static Jedis getJedis(){
//        Jedis jedis = new Jedis(host, port);
//
//        return jedis;
//    }

    public static Jedis getJedisPoll() {
        if (jedisPool == null) {
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            //最大可用连接数
            jedisPoolConfig.setMaxTotal(10);
            //最大闲置连接数
            jedisPoolConfig.setMaxIdle(5);
            //最小闲置连接数
            jedisPoolConfig.setMinIdle(5);
            //连接耗尽是否等待
            jedisPoolConfig.setBlockWhenExhausted(true);
            //等待时间
            jedisPoolConfig.setMaxWaitMillis(2000);

            //取连接的时候进行测试
            jedisPoolConfig.setTestOnBorrow(true);
            jedisPool = new JedisPool(jedisPoolConfig,host,port);



        }
        Jedis jedis = jedisPool.getResource();

        return jedis;
    }

    public static void testString(Jedis jedis){

        jedis.set("k1","123");

        String k1 = jedis.get("k1");
        System.out.println("get : "+ k1);

        Long strlen = jedis.strlen("k1");
        System.out.println("strlen : " + strlen);

        jedis.setnx("k2","v2");
        String k2 = jedis.get("k2");
        System.out.println("setnx : "+ k2);

        Long incr = jedis.incr("k1");

        System.out.println("incr : " + incr);

        Long incrBy = jedis.incrBy("k1", 10);

        System.out.println("incrBy : " + incrBy);

    }

    public static void testList(Jedis jedis) {
        jedis.lpush("l1","k1","v1","k2","v2");

        List<String> l1 = jedis.lrange("l1", 0, -1);
        System.out.println("lrange : " + l1);

        String lpop = jedis.lpop("l1");
        System.out.println("lpop : " + lpop);

        Long length = jedis.llen("l1");
        System.out.println("llen : " + length);

        Long linsert = jedis.linsert("l1", ListPosition.AFTER, "k2", "2223");

        System.out.println("linsert : " + linsert  + " , l1 : " + l1);

        String lindex = jedis.lindex("l1", 3);
        System.out.println("lindex : " + lindex);


    }

    public static void testSet(Jedis jedis) {
        jedis.sadd("s1", "3", "77", "2", "5", "8");

        Set<String> s1 = jedis.smembers("s1");
        System.out.println("smembers : " + s1 );

        Boolean sismember = jedis.sismember("s1", "3");
        System.out.println("sismember : " + sismember);

        Long scard = jedis.scard("s1");
        System.out.println("scard : " + scard);

        List<String> srandmember = jedis.srandmember("s1", 3);
        System.out.println("srandmember : " + srandmember);

        jedis.sadd("s11", "3", "1", "2");
        Set<String> s11 = jedis.smembers("s11");
        Set<String> sunion = jedis.sunion("s1", "s11");

        System.out.println("sunion : " + sunion);

    }

    public static void testZSet(Jedis jedis) {
        HashMap<String, Double> map = new HashMap<>();
        map.put("k1",1.0);
        map.put("k2",2.0);
        jedis.zadd("z11", map);
        Set<String> zrange = jedis.zrange("z11", 0, -1);
        System.out.println("zrange : " + zrange   );

        Set<String> zrevrange = jedis.zrevrange("z11", 0, -1);
        System.out.println("zrevrange : " + zrevrange);

        Long zcount = jedis.zcount("z11", 0, 20.0);
        System.out.println("zcount : " + zcount);

        Double zincrby = jedis.zincrby("z11", 20.0,  "k1");
        System.out.println("zincrby : " + zincrby);

        Set<String> zrevrangeByScore = jedis.zrevrangeByScore("z11", 20.0, 0);
        System.out.println("zrevrangeByScore : " + zrevrangeByScore);
    }

    public static void testHash(Jedis jedis) {
        HashMap<String, String> map = new HashMap<>();
        map.put("k1","123");
        map.put("k2","456");
        map.put("k3","789");


        jedis.hset("map1",map);

        Map<String, String> hgetAll = jedis.hgetAll("map1");
        System.out.println("hgetAll : " + hgetAll);

        Long hsetnx = jedis.hsetnx("map1", "k3", "101112");
        System.out.println("hsetnx : " + hsetnx);

        Set<String> hkeys = jedis.hkeys("map1");
        System.out.println("hkeys : " + hkeys);

        Boolean hexists = jedis.hexists("map1", "k1");
        System.out.println("hexists : " + hexists);

        Long hincrBy = jedis.hincrBy("map1", "k2", 1);
        System.out.println("hincrBy : " + hincrBy);
    }
}
