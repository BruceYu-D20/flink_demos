package redis_asyn;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class TestRedis {

    public static void main(String[] args) {

        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(30);
        config.setMaxIdle(10);

        JedisPool pool = new JedisPool(config, "172.16.108.183", 7000);

        Jedis conn = pool.getResource();

        System.out.println(conn.exists("hello"));

        System.out.println(conn.get("hello"));

        conn.close();
        pool.close();
    }
}
