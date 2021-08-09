# Spark项目学习笔记

#### 1. MPPDB数据库建表使用Range分区

 [MySQL RANGE分区](https://www.cnblogs.com/chenmh/p/5627912.html)

```sql
PARTITION BY RANGE(pt)
(
       PARTITION P20200101000500 VALUES LESS THAN(20200101000500)
)
```

#### 2. 分布式数据库分布键详解

[greenplum分布键详解](https://blog.csdn.net/double_happiness/article/details/83273054)

```sql
WITH (ORIENTATION = ROW )
DISTRIBUTE BY HASH (id,data_type)
```





扩展:  

[MPP架构数据库优化总结](https://blog.csdn.net/weixin_39781186/article/details/111783813?utm_medium=distribute.pc_relevant.none-task-blog-2~default~baidujs_title~default-0.control&spm=1001.2101.3001.4242) 

[雪球DB——Snowball ](https://www.modb.pro/wiki/632)

