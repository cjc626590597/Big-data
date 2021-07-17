## Spring JdbcTemplate

```java
		//1.获取容器
        ApplicationContext ac = new ClassPathXmlApplicationContext("bean.xml");
        //2.获取对象
        JdbcTemplate jt = ac.getBean("JdbcTemplate",JdbcTemplate.class);
        //3.执行操作
        //保存
        jt.update("insert into account(name,money)values(?,?)","aaa",1111);
        //更新
        jt.update("update account set name=?,money=? where id=?","bbb",2222,1);
        //删除
        jt.update("delete from account where id=?",9);
        //查询所有
		//需要创建数据库和类的映射，不推荐
        List<Account> accounts = jt.query("select * from account where money>?",
                                          new AccountRowMapper(),1000f);
		//直接导入存入的类，推荐
        List<Account> accounts = jt.query("select * from account where money>?",
                                          new BeanPropertyRowMapper<Account>(Account.class),1000f);
        for (Account account:accounts){
            System.out.println(account);
        }
        //查询一个
        List<Account> accounts = jt.query("select * from account where id=?",
                                          new BeanPropertyRowMapper<Account>(Account.class),1);
        System.out.println(accounts.isEmpty()?"没有内容":accounts.get(0));
        //查询返回一行一列（使用聚合函数，但不加group by字句）
        Long count = jt.queryForObject("select count(*) from account where money > ?",Long.class,1000f);
        System.out.println(count);
```

