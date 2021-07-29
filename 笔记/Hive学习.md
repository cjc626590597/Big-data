# Hive学习

Linux mysql安装 https://www.cnblogs.com/cheneyboon/p/14672581.html



/opt/graphdb/hugegraph-server/bin/start-hugegraph.sh

***\*验证\****，返回200表示server启动正常【***\*注意把IP换成服务器的IP\****】

	echo `curl -o /dev/null -s -w %{http_code} "http://172.25.21.17:8080/graphs/pcigraph/graph/vertices"`

/opt/graphdb/hugegraph-studio-0.11.0/bin/start-studio.sh



bin/hdfs namenode -format

/opt/module/hadoop-3.1.3/sbin/hadoop-daemon.sh start namenode

hdfs --daemon stop namenode

yarn --daemon stop resourcemanager

mapred --daemon stop historyserver

http://hadoop102:9870/explorer.html#/



/opt/module/zookeeper-3.4.10/bin/zkServer.sh start

/opt/module/kafka/bin/kafka-server-start.sh -daemon /opt/module/kafka/config/server.properties

```shell
//获取物理地址
vim /etc/udev/rules.d/70-persistent-net.rules
//修改ip地址
vim /etc/sysconfig/network-scripts/ifcfg-eth0
//修改主机名
vim /etc/sysconfig/network
//修改主机名映射
/etc/hosts
```



