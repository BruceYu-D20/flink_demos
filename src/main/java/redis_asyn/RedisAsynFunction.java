package redis_asyn;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Collections;
import java.util.concurrent.ExecutorService;

public class RedisAsynFunction extends RichAsyncFunction<String, String> {


    private JedisPool jedisPool;
    private transient ExecutorService executorService;

    @Override
    public void open(Configuration parameters) throws Exception {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(30);
        config.setMaxIdle(10);

        jedisPool = new JedisPool(config, "172.16.108.183", 7000);
    }

    @Override
    public void asyncInvoke(String key, ResultFuture<String> resultFuture) throws Exception {


        executorService.submit(new Runnable() {
            @Override
            public void run() {
                try{
                    Jedis jedis = jedisPool.getResource();
                    if(jedis.exists(key)){
                        resultFuture.complete(Collections.singleton(jedis.get(key)));
                    }

                }catch (Exception e){
                    e.printStackTrace();
                }

            }

        });
    }

    @Override
    public void close() throws Exception {
        jedisPool.close();
    }
}
