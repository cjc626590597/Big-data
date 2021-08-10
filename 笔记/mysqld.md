# mysql知识点

- mysql -uroot -pa123456
- show databases;
- create database test charset=utf8;
- use test;  ！！！
- show tables;

#### 4. 创建数据库表

- CREATE TABLE pet(
      name VARCHAR(20),
      species VARCHAR(20),
      sex CHAR(1),
      birth DATE,

#### 5. 增加数据记录

- INSERT INTO pet

  VALUES('旺财','狗','公','1999-01-01');

- select * from pet;

- INSERT INTO pet VALUES('ass','dog','f','1998-02-03');

  INSERT INTO pet VALUES('baa','cat','m','1997-11-11');

  INSERT INTO pet VALUES('ccc','bird','m','2000-03-08');

  INSERT INTO pet VALUES('dee','cat','f','1999-01-01');

  INSERT INTO pet VALUES('eww','dog','m','1998-08-08');

  INSERT INTO pet VALUES('fff','bird','m','1997-04-04');

#### 7. 增删查改操作

- delete from pet where name='fff';

- update pet set name='bbb' where birth='1997-11-11';

#### 8. 主键约束

- create table user(

  id int primary key,

  name char(20)

  );

- desc user

- insert into user values(1,'张三')；

  insert into user values(1,'李四')；报错

- select * from user

#### 9. 联合主键约束

- create table user(

  id int ,

  name char(20),

  password varchar(20),

  primary  key(id,name)

  );

#### 10. 自增约束，自动添加ID

- create table user3(

  id int  primary key auto_increment,

  name varchar(20)

  )

#### 11. 忘记创建主键约束

- create table user(

  id int primary key,

  name char(20)

  );

- alter table user add primary(id);

- alter table user drop primary(id);

- alter table user modify id int primary key;

#### 12. 唯一约束，字段的值不可以重复

- alter table user add unique(name)

- create table user(

  id int ,

  name char(20) //unique

  //unique name

  );

- alter table user drop index name;

- alter table user  name varchar(20) unique;

#### 13. 非空约束 修饰的字段不能为空Null

- not null 

#### 14. 默认约束 插入字段时，没有传值，就会使用默认值

- create table user(

  id int ,

  name char(20) 

  age int default 10

  );

#### 15. 外键约束 涉及到两个表，主表和副表

- create table classes(

  id int primary key,

  name varchar(20),

  );

- create table students(

  id int primary key ,

  name varchar(20),

  class_id int,

  foreign key(class_id) references classes(id)

  )

- insert into classes values(1,"一班")

  insert into classes values(2,"二班")

  insert into students values(1001,"一班"，1)

  insert into students values(1002,"二班"，2)

  insert into students values(1002,"二班"，3) ERROR

- 主表中classes没有的数据值，子表中不可以使用；主表记录被引用时，不可以删除元素

- 数据库的三大设计范式

#### - 第一范式

- 数据表中的所有字段都是不可分隔的原子值
- 例如地址可以修改为国家，省份，城市，具体地址

#### - 第二范式

- 满足第一范式，除主键外的每一列都必须完全依赖主键，如果不满足完全依赖，只能发生在联合主键的情况下。 

  <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20200512120532857.png" alt="image-20200512120532857" style="zoom:100%;" />

#### - 第三范式

- 满足第二范式，除开主键列的其他列之间不能有传递依赖关系

#### 19. 数据准备

- create database selectTest

- create table student(

  sno varchar(20) primary key,

  sname varchar(20) not null,

  ssex varchar(20) not null,

  sbirthday datetime,

  class varchar(20)

  );

- create table teacher(

  tno varchar(20) primary key,

  tname varchar(20) not null,

  tsex varchar(10) not null,

  tbirthday datetime,

  prof varchar(20) not null,

  depart varchar(20) not null

  );

- create table course(

  cno varchar(20) primary key,

  cname varchar(20) not null,

  tno varchar(20) not null,

  foreign key(tno) references teacher(tno) 

   );

- create table score(

  sno varchar(20) not null,

  cno varchar(20) not null,

  degree decimal,

  foreign key(sno) references student(sno),

  foreign key(cno) references course(cno),

  primary key(sno,cno)

  );

- insert into student values('101','曾华','男','1977-09-01','95033');

  insert into student values('102','匡明','男','1975-10-02','95031');

  insert into student values('103','王丽','女','1976-01-23','95033');

  insert into student values('104','李军','男','1976-02-20','95033');

  insert into student values('105','王芳','女','1975-02-10','95031');

  insert into student values('106','陆军','男','1974-06-03','95031');

  insert into student values('107','王尼玛','男','1976-02-20','95033');

  insert into student values('108','张全蛋','女','1975-02-10','95031');

  insert into student values('109','赵铁柱','男','1974-06-03','95031');

- insert into teacher values('804','李诚','男','1958-12-02','副教授','计算机系');

  insert into teacher values('856','张旭','男','1969-03-12','讲师','电子工程系');

  insert into teacher values('825','王萍','女','1972-05-05','助教','计算机系');

  insert into teacher values('831','刘冰','女','1977-08-14','助教','电子工程系');

- insert into course values('3-105','计算机导论','825');

  insert into course values('3-245','操作系统','804');

  insert into course values('6-166','数字电路','856');

  insert into course values('9-888','高等数学','831');

- insert into score values('103','3-105','92');

  insert into score values('103','3-245','86');

  insert into score values('103','6-166','85');

  insert into score values('105','3-105','88');

  insert into score values('105','3-245','75');

  insert into score values('105','6-166','79');

  insert into score values('109','3-105','76');

  insert into score values('109','3-245','68');

  insert into score values('109','6-166','81');
  
- insert into score values('101','3-105','90');

  insert into score values('102','3-105','91');

  insert into score values('104','3-105','89');

#### 20. 查询练习1-10

- 1.查询student表中所有的记录

  select * from student;

- 2.查询student表中的所有记录的sname、ssex和class列

- select sname,ssex,class from student;

- 3.查询教师虽有的单位即不重复的depart列

  select distinct depart from teacher;

- 4.查询score表中成绩在60到80之间的所有记录

  select * from score where degree between 60 and 80;

  select * from score where degree > 60 and degree < 80;

- 5.查询score表中成绩为85, 86, 或者88的记录

  select * from score where degree in(85,86,88);

- 6.查询student表中'95031'班或者性别为'女'的同学记录

  select * from student where class = '95031' or ssex =' 女';

- 7.以class降序查询student表中所有的记录

  select * from student order by class desc; asc(默认升序)

- 8.以c_no升序.sc_degree降序查询score表中所有的数据

  select * from score order by cno asc, degree desc;

- 9.查询'95031'班的学生人数

  select count(*) from student where class = '95031';

- 10.查询score表中的最高分数的学生号和课程号.(子查询或者排序)

  select  sno,cno from score where degree = (select max(degree) from score);

  select sno,cno,degree from score order by degree desc limit 0,1;

  limit 第一个数字表示从读多少开始，后一个数字表示多少条

- 11.查询每门课的平均成绩

  select avg(degree) from score where cno='3-105';

  select cno,avg(degree) from score group by cno;

- 12.查询score表中至少有2名学生选修的,并且以3开头的课程的平均分

  select cno,avg(degree),count(*) from score group by cno 

  having count(cno)>=2 and cno like '3%';

- 13.查询分数大于70但是小于90的s_no列:

  select sno,degree from score

  where degree>70 and degree<90;  // where degree between 60 and 90;

- 14.查询所有的学生 s_name , c_no, sc_degree列

  select sname,cno,degree from student,score

  where student.sno= score.sno;

- 15.查询所有学生的s_no, c_name, sc_degree列

- select sno,cname,degree from course,score

  where course.cno=score.cno;

- 16.查询所有的学生 s_name , c_name, sc_degree列

  select sname,cname,degree from student,course,score

  where student.sno=score.sno and score.cno=course.cno;

- 17.查询班级是'95031'班学生每门课的平均分

  select * from student where class='95031';

  select * from score where 

  sno in (select sno from student where class='95031');

  select cno,avg(degree) from score 

  where sno in (select sno from student where class='95031')

  group by cno;

- 18.查询选修"3-105"课程的成绩高于'109'号同学'3-105'成绩 的所有同学的记录

  select * from score where cno='3-105' and degree>

  (select degree from score where sno='109' and cno='3-105');

- 19.查询成绩高于学号为'109',课程号为'3-105'的成绩的所有记录

- select * from score where degree>

  (select degree from score where sno='109' and cno='3-105');

- 20.查询所有学号为108.101的同学同年出生的所有学生的s_no,s_name和s_birthday

  select * from student where year(sbirthday) in

  (select year(sbirthday) from student where sno in(108,101));

- 21.查询 张旭 教师任课的学生的成绩

  select * from score where cno=

  (select cno from course where tno=

  (select tno from teacher where tname='张旭'));

- 22.查询选修课程的同学人数多余 5 人的教师姓名

  insert into score values('101','3-105','90');

  insert into score values('102','3-105','91');

  insert into score values('104','3-105','89');

  select tname from teacher where tno=

  (select tno from course where cno=

  (select cno from score group by cno having count(*)>5));

- 23.查询95033班和95031班全体学生的记录

  insert into student values('110','张飞','男','1974-06-03','95038');

  select * from student where class in ('95031','95033');

- 24.查询存在85分以上成绩的课程c_no

  select cno,degree from score where degree>85;

- 25.查出所有'计算机系' 教师所教课程的成绩表

  select * from score where cno in 

  (select cno from course where tno in 

  (select tno from teacher where depart = '计算机系'));

- 26.查询'计算机系'与'电子工程系' 不同职称的教师的name和rof

  select * from teacher where depart = '计算机系' and prof not in (select prof  from teacher where depart = '电子工程系' )

  union//求并集

  select * from teacher where depart = '电子工程系' and prof not in (select prof  from teacher where depart = '计算机系' );

- 27, 查询选修编号为"3-105"课程且成绩至少高于选修编号为'3-245'同学的c_no,s_no和sc_degree,并且按照sc_degree从高到地次序排序

  select cno,sno,degree from score where cno='3-105' and degree >any (

  select degree from score where cno = '3-245')

  order by degree desc;

- 28.查询选修编号为"3-105"且成绩高于选修编号为"3-245"课程的同学c_no.s_no和sc_degree

  select cno,sno,degree from score where cno='3-105' and degree >all (

  select degree from score where cno = '3-245')

  order by degree desc;

- 29.查询所有教师和同学的 name ,sex, birthday

  select tname as name,tsex as sex,tbirthday as birthday from teacher

  union

  select sname,ssex,sbirthday from student;

- 30.查询所有'女'教师和'女'学生的name,sex,birthday

  select tname as name,tsex as sex,tbirthday as birthday from teacher where tsex='女'

  union

  select sname,ssex,sbirthday from student where ssex='女';

- 31.查询成绩比该课程平均成绩低的同学的成绩表

  select * from score a where degree <

  (select avg(degree) from score b  where a.cno=b.cno group by cno);

- 32.查询所有任课教师的t_name 和 t_depart(要在分数表中可以查得到)

  select tname,depart from teacher where tno in (

  select tno from course);

- 33.查出至少有2名男生的班号

  select class from student where ssex='男' group by class having count(*)>1; 

- 34.查询student 表中 不姓"王"的同学的记录

  select * from student where sname not like '王%';

- 35.查询student 中每个学生的姓名和年龄(当前时间 - 出生年份)

  select sname,year(now())-year(sbirthday) as '年龄'from student;

- 36.查询student中最大和最小的 s_birthday的值

  select max(sbirthday) as '最大',min(sbirthday) as '最小' from student;

- 37.以班级号和年龄从大到小的顺序查询student表中的全部记录

  select * from student order by class desc,sbirthday;

- 38.查询"男"教师 及其所上的课

  select * from course where tno in (select tno from teacher where tsex='男');

- 39.查询最高分同学的s_no c_no 和 sc_degree;

  select * from score where degree in (select max(degree) from score);

- 40.查询和"李军"同性别的所有同学的s_name

  select sname from student where ssex=(select ssex from student where sname='李军');

- 41.查询和"李军"同性别并且同班的所有同学的s_name

  select sname from student where ssex=(select ssex from student where sname='李军') and class=(select class from student where sname='李军');

- 42.查询所有选修'计算机导论'课程的'男'同学的成绩表

  select * from score where cno=(select cno from course where cname='计算机导论') and sno in (select sno from student where ssex = '男');

- 43.假设使用了以下命令建立了一个grade表

  create table grade(

  low int(3),

  upp int(3),

  grade char(1)

  );

  insert into grade values (90,100,'A');

  insert into grade values (80,89,'B');

  insert into grade values (70,79,'C');

  insert into grade values (60,69,'D');

  insert into grade values (0,59,'E');

- 查询所有同学的s_no , c_no 和grade列

  select sno,cno,grade from score,grade where degree between low and upp;

#### 21连接查询

- 内连接inner join 或者 join
- 外连接
- left(right) join 或者 left(right)  outer join 

- 完全外连接 full join 或者full outer join

- create database testJoin;

- CREATE TABLE person ( 

  id INT,    

  name VARCHAR(20),    

  cardId INT 

  );

- CREATE TABLE card (    

  id INT, 

  name VARCHAR(20) 

  );

- INSERT INTO card VALUES (1, '饭卡'), (2, '建行卡'), (3, '农行卡'), (4, '工商卡'), (5, '邮政卡'); 

- INSERT INTO person VALUES (1, '张三', 1), (2, '李四', 3), (3, '王五', 6); 

- select *  from person inner join card on person.cardID = card.id;   内连接

- select *  from person join card on person.cardID = card.id;

- select *  from person left join card on person.cardID = card.id;  左外连接，左表一定会显示，右边缺少的补null

- select *  from person right join card on person.cardID = card.id;  右外连接，右表一定会显示，左边缺少的补null

- select *  from person full join card on person.cardID = card.id;  mysql不支持full join

- 类似于

  select *  from person left join card on person.cardID = card.id union

  select *  from person right join card on person.cardID = card.id;

#### 21mysql事务

- 事务是一个最小的不可分割工作单元，保证业务的完整性。

- ```
  -- a -> -100
  UPDATE user set money = money - 100 WHERE name = 'a';
  
  -- b -> +100
  UPDATE user set money = money + 100 WHERE name = 'b';
  ```

  在实际项目中，假设只有一条 SQL 语句执行成功，而另外一条执行失败了，就会出现数据前后不一致。

  因此，在执行多条有关联 SQL 语句时，**事务**可能会要求这些 SQL 语句要么同时执行成功，要么就都执行失败。

- 在 MySQL 中，事务的**自动提交**状态默认是开启的。

- **自动提交**

  - 查看自动提交状态：`SELECT @@AUTOCOMMIT` ；
  - 设置自动提交状态：`SET AUTOCOMMIT = 0` 。

- **手动提交**

  `@@AUTOCOMMIT = 0` 时，使用 `COMMIT` 命令提交事务。

- **事务回滚**

  `@@AUTOCOMMIT = 0` 时，使用 `ROLLBACK` 命令回滚事务。

#### 22手动开启事务 - BEGIN / START TRANSACTION

- BEGIN; 
- UPDATE user set money = money - 100 WHERE name = 'a'; UPDATE user set money = money + 100 WHERE name = 'b';

- ROLLBACK;可以回滚
- COMMIT;之后就不能回滚

#### 23事务的四大特性

- **A 原子性**：事务是最小的单位，不可以再分割；
- **C 一致性**：要求同一事务中的 SQL 语句，必须保证同时成功或者失败；
- **I 隔离性**：事务1 和 事务2 之间是具有隔离性的；
- **D 持久性**：事务一旦结束 ( `COMMIT` ) ，就不可以再返回了 ( `ROLLBACK` ) 。

#### 24事务的隔离性

- **READ UNCOMMITTED ( 读取未提交 )**

  如果有多个事务，那么任意事务都可以看见其他事务的**未提交数据**。

- **READ COMMITTED ( 读取已提交 )**

  只能读取到其他事务**已经提交的数据**。

- **REPEATABLE READ ( 可被重复读 )**

  如果有多个连接都开启了事务，那么事务之间不能共享数据记录，否则只能共享已提交的记录。

- **SERIALIZABLE ( 串行化 )**

  所有的事务都会按照**固定顺序执行**，执行完一个事务后再继续执行下一个事务的**写入操作**。

**脏读**，一个事务读取到另外一个事务还未提交的数据。这在实际开发中是不允许出现的。

虽然 **READ COMMITTED** 让我们只能读取到其他事务已经提交的数据，但还是会出现问题，就是**在读取同一个表的数据时，可能会发生前后不一致的情况。\**这被称为\**不可重复读现象 ( READ COMMITTED )** 。

报错了，操作被告知已存在主键为 `6` 的字段。这种现象也被称为**幻读，一个事务提交的数据，不能被其他事务读取到**。

此时会发生什么呢？由于现在的隔离级别是 **SERIALIZABLE ( 串行化 )** ，串行化的意思就是：假设把所有的事务都放在一个串行的队列中，那么所有的事务都会按照**固定顺序执行**，执行完一个事务后再继续执行下一个事务的**写入操作** ( **这意味着队列中同时只能执行一个事务的写入操作** ) 。



根据这个解释，小王在插入数据时，会出现等待状态，直到小张执行 `COMMIT` 结束它所处的事务，或者出现等待超时。

#### 25. sql笔记

1. 排名(如果无法用窗口函数则通过自连接和<=号配合COUNT函数找出排名)
   - RANK() 排序相同时会重复，总数不会变  
   - DENSE_RANK() 排序相同时会重复，总数会减少
   - ROW_NUMBER() 会根据顺序计算

2. 判断是否为空 is null
3. 查找包含字符robot的... like '%robot%'
4. 查找种类数量大于5的 group by categoryID having count(*) > 5
5. 连接字符 concat.ws(','    , str1, str2) 
6. 常用的三种插入数据的语句:
   - insert into表示插入数据，数据库会检查主键，如果出现重复会报错
   - replace into表示插入替换数据，需求表中有PrimaryKey，或unique索引，如果数据库已经存在数据，则用新数据替换，如果没有数据效果则和insert into一样
   - insert ignore表示，如果中已经存在相同的记录，则忽略当前新数据；insert ignore into actor values("3","ED","CHASE","2006-02-15 12:34:33")

7. 创建数据表的三种方法
   - 常规创建
     create table if not exists 目标表
   - 复制表格
     create 目标表 like 来源表
   - 将table1的部分拿来创建table2
     create table if not exists actor_name(first_name varchar(45) not null, last_name varchar(45) not null)
     (insert into actor_name) select first_name,last_name from actor

8. 创建索引的三种方法
   - CREATE UNIQUE INDEX indexName ON 表名(列名(length)) 
   - ALTER 表名 ADD UNIQUE [indexName] ON (列名(length)) 
   CREATE TABLE mytable(
   ID INT NOT NULL,
   username VARCHAR(16) NOT NULL,
   UNIQUE [indexName] (username(length))


9. 创建视图  create view actor_name_view AS select first_name AS first_name_v, last_name AS last_name_v from actor

10. 使用强制索引查询
    - MYSQL中强制索引查询使用：FORCE INDEX(indexname);
    - SQLite中强制索引查询使用：INDEXED BY indexname;

11. 添加一列 Alter table actor add column create_date datetime NOT NULL default '2020-10-01 00:00:00'

12. 创建一个诱发器
    - create trigger audit_log
      after insert on employees_test
      begin 
       INSERT INTO audit VALUES (new.id, new.name);
      end

13. 删除数据 delete from test where id in (select * from (select id from test where id = 2) t )

14. MySQL中ALTER TABLE 的命令用法
    - ALTER TABLE 表名 ADD 列名/索引/主键/外键等；
    - ALTER TABLE 表名 DROP 列名/索引/主键/外键等；
    - ALTER TABLE 表名 ALTER 仅用来改变某列的默认值；
    - ALTER TABLE 表名 RENAME 列名/索引名 TO 新的列名/新索引名；
    - ALTER TABLE 表名 RENAME TO/AS 新表名;
    - ALTER TABLE 表名 MODIFY 列的定义但不改变列名；
    - ALTER TABLE 表名 CHANGE 列名和定义都可以改变。

15. 创建外键 FOREIGN KEY(emp_no) REFERENCES employees_test(id)
