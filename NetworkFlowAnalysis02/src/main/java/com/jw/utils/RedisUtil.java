package com.jw.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.Serializable;

public class RedisUtil implements Serializable {
    private static JedisPool jedisPool = null;

    private RedisUtil() {}

    public static Jedis getJedis() {
        return InnerRedis.jedis;
    }

    private static class InnerRedis implements Serializable {
        private static final Jedis jedis = getJedisClient();
    }

    private static Jedis getJedisClient() {
        if (jedisPool == null) {
            String host = "localhost";
            int port = 6379;
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            jedisPoolConfig.setMaxTotal(100);  //最大连接数
            jedisPoolConfig.setMaxIdle(20);   //最大空闲
            jedisPoolConfig.setMinIdle(20);     //最小空闲
            jedisPoolConfig.setBlockWhenExhausted(true);  //忙碌时是否等待
            jedisPoolConfig.setMaxWaitMillis(500);//忙碌时等待时长 毫秒
            jedisPoolConfig.setTestOnBorrow(true); //每次获得连接的进行测试
            jedisPool = new JedisPool(jedisPoolConfig, host, port);
        }
        return jedisPool.getResource();
    }

//    public static void main(String[] args) {
//        Jedis jedis = getJedisClient();
//        String ping = jedis.ping();
//        System.out.println(ping);
//    }
}
