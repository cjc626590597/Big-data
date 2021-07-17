### Hadoop运行环境搭建

root密码：a123456

#### 虚拟机环境搭建

- 到70-persistent-net.rules 删除etho0, 把eth1的物理地址复制，并将其修改为eth0

  vim /etc/udev/rules.d/70-persistent-net.rules

- 到ifcfg-eth0中，将物理地址修改为复制的，然后修改ip地址

  vim /etc/sysconfig/network-scripts/ifcfg-eth0

- 到network中修改主机名字

  vim /etc/sysconfig/network

- 查看hosts名字，不要重复

  vim /etc/hosts



- 格式化namenode

bin/hdfs namenode -format

- 开启集群 

  ```shel
  sbin/hadoop-daemon.sh start namenode
  sbin/hadoop-daemon.sh start datanode
  sbin/yarn-daemon.sh start resourcemanager
  sbin/yarn-daemon.sh start nodemanager
  sbin/mr-jobhistory-daemon.sh start historyserver
  ```

  