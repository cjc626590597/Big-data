package com.suntek.algorithm.process.util;

import java.util.*;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

public class RedisOperator {
    private JedisCluster jedisCluster;

    public RedisOperator(String redisString) {
//        String   redisString = "172.25.21.104:6379";
        String[] hostArray = redisString.split(",");
        Set<HostAndPort> nodes = new HashSet<HostAndPort>();

        //配置redis集群
        for (String host : hostArray) {
            String[] detail = host.split(":");
            nodes.add(new HostAndPort(detail[0], Integer.parseInt(detail[1])));
        }

        jedisCluster = new JedisCluster(nodes);
    }

    public TreeSet<String> keys(String pattern) {
        System.out.println("Start getting keys...");
        TreeSet<String> keys = new TreeSet<>();
        Map<String, JedisPool> clusterNodes = jedisCluster.getClusterNodes();   //获取所有连接池节点
        for (String k : clusterNodes.keySet()) {  //遍历所有连接池，逐个进行模糊查询
            System.out.println("Getting keys from:" + k);
            JedisPool jp = clusterNodes.get(k);
            Jedis connection = jp.getResource();
            try {
                keys.addAll(connection.keys(pattern));//获取Jedis对象，Jedis对象支持keys模糊查询  
            } catch (Exception e) {
                System.out.println("Getting keys error:" + e);
            } finally {
                System.out.println("Connection closed.");
                connection.close();//用完一定要close这个链接！！！  
            }
        }
        System.out.println("Keys gotten!");
        return keys;
    }


//    public void delete(String pattern) {
//        System.out.println("Start getting keys...");
//
//        Map<String, JedisPool> clusterNodes = jedisCluster.getClusterNodes();   //获取所有连接池节点
//        String deleteScript ="local keys,values=KEYS,ARGV \n" +
//                "local array = {}" +
//                "   for i,v in ipairs(keys) do\n" +
//                "         array[i] = redis.call('del',keys[i]) \n" +
//                "   end " +
//                "return array;";
//
//        for (String k : clusterNodes.keySet()) {  //遍历所有连接池，逐个进行模糊查询
//            System.out.println("Getting keys from:" + k);
//            JedisPool jp = clusterNodes.get(k);
//            Jedis connection = jp.getResource();
//            try {
//                TreeSet<String> keys = new TreeSet<>();
//                keys.addAll(connection.keys(pattern));//获取Jedis对象，Jedis对象支持keys模糊查询
////
//                if(keys.size()>0) {
//                    System.out.println();
//                }
//                HashMap<String,ArrayList> map  = new HashMap<>();
//                for(String key :keys) {
//                    String[] strs  = key.split("}");
//                    String kk =strs[0]+"}";
//                    ArrayList list = map.getOrDefault(kk,new ArrayList());
//                    list.add(key);
//                    map.put(kk,list);
//                }
//
//                 for(ArrayList<String> al:map.values()) {
//                     connection.eval(deleteScript,al,new ArrayList());
//                 }
//
//            } catch (Exception e) {
//                System.out.println("Getting keys error:" + e);
//            } finally {
//                System.out.println("Connection closed.");
//                connection.close();//用完一定要close这个链接！！！
//            }
//        }
////        jedisCluster.eval(deleteScript,);
//        System.out.println("Keys gotten!");
//
//    }

    /**
     * 获取redis中指定key的值，value类型为String的使用此方法
     */
    public String get(String key) {
        return jedisCluster.get(key);
    }

    /**
     * 设置redis中指定key的值，value类型为String的使用此方法
     */
    public void set(String key, String value) {
        jedisCluster.set(key, value);
    }

    /**
     * 获取redis中指定key的值,对应的value，value类型为MAP的使用此方法
     */
    public Map<String, String> getMap(String key) {
        return jedisCluster.hgetAll(key);
    }

    /**
     * 删除redis中指定key的值项
     */
    public void del(String key) {
        jedisCluster.del(key);
    }

    public static void main(String[] args) {
        //172.25.22.43:7000,172.25.22.43:7001,172.25.22.43:7002,172.25.22.44:7000,172.25.22.44:7001,172.25.22.44:7002,172.25.22.45:7000,172.25.22.45:7001,172.25.22.45:7002
        //das:trackstatic*
        if(args.length!=3){
            System.out.println("请输入3个参数，redis Ip地址 操作类型[COUNT]  key值  ，格式如下：");
            System.out.println("ip:port  COUNT das:trackstatic*");
            return;
        }
        RedisOperator ro = new RedisOperator(args[0]);
       String oper = args[1];
       String key = args[2];
        if(oper.equals("COUNT")){
           TreeSet<String> ts = ro.keys(key);
           System.out.println("["+key+"]" +" 的条数为：" +ts.size());
       }else {
           System.out.println("请输入操作 删除  ip:port  COUNT  key(模糊用*) ");
       }
    }

}
