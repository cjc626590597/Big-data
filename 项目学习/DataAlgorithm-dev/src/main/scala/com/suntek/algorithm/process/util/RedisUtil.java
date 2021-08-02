package com.suntek.algorithm.process.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.*;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Author: Felix
 * Date: 2021/2/5
 * Desc: 通过JedisPool连接池获取Jedis连接
 */
public class RedisUtil {
    private  Jedis jedis;
    private JedisClusterPipeline jcp;
    private JedisCluster jedisCluster;
    private Pipeline pipeline;


    public RedisUtil(String conectorStr, String password, int dbIndex){
        if(conectorStr.contains(",")) {
            JedisCluster jedisCluster =  getJedisCluster(conectorStr, password);
            jcp = new JedisClusterPipeline(jedisCluster);
            jcp.refreshCluster();
        }else {
            String[]  strs =  conectorStr.split(":");
            this.pipeline =  getJedis2(strs[0], Integer.parseInt(strs[1]), password, dbIndex);
        }
    }
    public void set(String key,String value) {
        if(this.pipeline !=null) {
            pipeline.set(key,value);

        }else if(this.jcp !=null) {
            jcp.set(key,value);
        }
    }


    public void setex(String key,int secend,String value) {
        if(this.pipeline !=null) {
            pipeline.setex(key,secend,value);

        }else if(this.jcp !=null) {
            jcp.setex(key,secend,value);
        }
    }

    public void sync() {
        if(this.pipeline !=null) {
             pipeline.sync();
        }else if(this.jcp !=null) {
            jcp.sync();
        }
    }

//    public Object eval(String script,List<String> keys,List<String> values) {
//        Object obj = null;
//
//         if(this.jedis!=null) {
//             obj = jedis.eval(script,keys,values);
//         }else if( this.jcp!= null) {
//             obj = jedisCluster.eval(script,keys,values);
//         }
//        return obj;
//    }

    public  void close(){
        if(this.jedis!=null) {
            jedis.close();
        }
        if( this.jedisCluster!= null) {
            try {
                jedisCluster.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if(this.jcp !=null) {
            this.jcp.close();
        }

    }

    public    Jedis getJedis(String host,int port){
            JedisPool jedisPool = null;
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            jedisPoolConfig.setMaxTotal(100); //最大可用连接数
            jedisPoolConfig.setBlockWhenExhausted(true); //连接耗尽是否等待
            jedisPoolConfig.setMaxWaitMillis(2000000); //等待时间
            jedisPoolConfig.setMaxIdle(5); //最大闲置连接数
            jedisPoolConfig.setMinIdle(5); //最小闲置连接数
            jedisPoolConfig.setTestOnBorrow(true); //取连接的时候进行一下测试 ping pong

            jedisPool = new JedisPool(jedisPoolConfig,host,port,2000000);

        return jedisPool.getResource();
    }

    public   JedisCluster getJedisCluster(String hostPorts, String password) {

        Set<HostAndPort> jedisClusterNodes = new HashSet<>();
        for(String str :hostPorts.split(",")) {
            String[] hostPort = str.split(":");
            jedisClusterNodes.add(new HostAndPort(hostPort[0], Integer.parseInt(hostPort[1])));
        }

        if (StringUtils.isBlank(password)) {
            password = null;
        }

        return new JedisCluster(jedisClusterNodes, 10000, 10000, 3, password, new GenericObjectPoolConfig());
    }

    public static Pipeline getJedis2(String host, int port, String password, int dbIndex){

        JedisShardInfo jedisShardInfo = new JedisShardInfo(host, port);
        jedisShardInfo.setConnectionTimeout(10000);
        jedisShardInfo.setSoTimeout(10000);
        if (!StringUtils.isBlank(password)) {
            jedisShardInfo.setPassword(password);
        }
        Jedis jedis = new Jedis(jedisShardInfo);
        jedis.select(dbIndex);
        return jedis.pipelined();
    }




    public static void main(String[] args) {
        String script = "local keys,values=KEYS,ARGV \n" +
                "local array = {}" +
                "   for i,v in ipairs(keys) do\n" +
                "         array[i] = redis.call('SET',keys[i],values[i]) \n" +
                "   end " +
                "return array;";

//        Jedis jedis = getJedis("172.25.21.2",6379);
//        System.out.println(jedis.ping());
//       JedisCluster jc =  getJedisCluster("172.25.22.43:7000,172.25.22.43:7001,172.25.22.43:7002,172.25.22.44:7000,172.25.22.44:7001,172.25.22.44:7002,172.25.22.45:7000,172.25.22.45:7001,172.25.22.45:7002");
//        Jedis jc = getJedis("172.25.21.104",6379);
//       jc.set("test","ab");
//        System.out.println(jc.get("test"));
//        String prefix = "luffi:lbl";
//        String KEY_SPLIT = "";
//
//        ArrayList key = new ArrayList(){{
//            add("{das:trackstatic}:3:1368830069933477890");
//            add("{das:trackstatic}:2:1368830069933477891");
//        }};
//        ArrayList value = new ArrayList(){{
//            add("{\"first_device_type\":6,\"data_type\":\"3\",\"id\":\"1368830069933477890\",\"last_device_type\":6,\"last_time\":20210308160430,\"first_time\":20210308160430,\"cnt\":1,\"all_cnt\":1,\"first_device_id\":\"44010663881971210119\",\"last_device_id\":\"44010663881971210119\"}");
//            add("{\"first_device_type\":6,\"data_type\":\"3\",\"id\":\"1368830069933477890\",\"last_device_type\":6,\"last_time\":20210308160430,\"first_time\":20210308160430,\"cnt\":1,\"all_cnt\":1,\"first_device_id\":\"44010663881971210119\",\"last_device_id\":\"44010663881971210119\"}");
//        }};
//
//        jc.eval(script,key,value);
//        System.out.println(jc.get("{das:trackstatic}:3:1368830069933477890"));
//        System.out.println(jc.get("{das:trackstatic}:2:1368830069933477891"));

//        jc.mset("sf","d","aadf","as");


//        String nameKey = prefix + KEY_SPLIT + "name";
//
//        String result = jc.mset("{" + prefix + KEY_SPLIT + "}" + "name", "张三", "{" + prefix + KEY_SPLIT + "}" + "age", "23", "{" + prefix + KEY_SPLIT + "}" + "address", "adfsa", "{" + prefix + KEY_SPLIT + "}" + "score", "100");
//        System.out.println(result);
//
//        String name = jc.get("{" + prefix + KEY_SPLIT + "}" + "name");
//        System.out.println(name);
//
//        Long del = jc.del("{" + prefix + KEY_SPLIT + "}" + "age");
//        System.out.println(del);
//
//        List<String> values = jc.mget("{" + prefix + KEY_SPLIT + "}" + "name", "{" + prefix + KEY_SPLIT + "}" + "age", "{" + prefix + KEY_SPLIT + "}" + "address");
//        System.out.println(values);



    }
}
