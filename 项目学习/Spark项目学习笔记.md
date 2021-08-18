# Spark项目学习笔记

### 一. MPPDB数据库建表使用Range分区

 [MySQL RANGE分区](https://www.cnblogs.com/chenmh/p/5627912.html)

```sql
PARTITION BY RANGE(pt)
(
       PARTITION P20200101000500 VALUES LESS THAN(20200101000500)
)
```

### 二. 分布式数据库分布键详解

[greenplum分布键详解](https://blog.csdn.net/double_happiness/article/details/83273054)

```sql
WITH (ORIENTATION = ROW )
DISTRIBUTE BY HASH (id,data_type)
```

扩展:  

[MPP架构数据库优化总结](https://blog.csdn.net/weixin_39781186/article/details/111783813?utm_medium=distribute.pc_relevant.none-task-blog-2~default~baidujs_title~default-0.control&spm=1001.2101.3001.4242) 

[雪球DB——Snowball ](https://www.modb.pro/wiki/632)

### 三.相似度算法

衡量相似度的方法有很多：欧式距离，动态时间规划DTW，编辑距离EDR，最长公共子序列，最大时间出现法MCT，余弦相似性，Hausdorff距离。其中基于轨迹数据衡量相似度的算法有三种：欧式距离，DTW算法，LCSS算法。

#### 1.LCSS算法（最长公共子序列算法）

 [LCSS最长公共子序列算法](https://www.cnblogs.com/hugechuanqi/p/10642684.html)

具体实现步骤查看参考论文《一种卡口车辆轨迹相似度算法的研究和实现》

DM_RELATION_DISTRIBUTE_DETAIL表中存放了LCSS算法实现的最长公共子序列的记录

#### 2.动态时间规整算法（DTW）

**DTW算法的步骤为：**

1. **计算两个序列各个点之间的距离矩阵。**

2. **寻找一条从矩阵左上角到右下角的路径，使得路径上的元素和最小。**

具体实现步骤查看参考论文《SparseDTW: A Novel Approach to Speed up Dynamic Time Warping》

[动态时间规整（DTW）算法简介](https://zhuanlan.zhihu.com/p/43247215)

[语音信号处理之（一）动态时间规整（DTW）](https://blog.csdn.net/zouxy09/article/details/9140207)

