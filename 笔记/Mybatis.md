## Maven项目配置

创建Maven项目时需要修改以下内容

 <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20201228175413645.png" alt="image-20201228175413645" style="zoom: 50%;" />![image-20201228175447172](C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20201228175447172.png)

 <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20201228175627483.png" alt="image-20201228175627483" style="zoom:50%;" />

-DarchetypeCatalog=local



## 导入依赖包

缺少的依赖包 去[Maven Repository: Search/Browse/Explore (mvnrepository.com)](https://mvnrepository.com/)查找然后复制maven配置文件Pom就可以自动下载了

 <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210104175456610.png" alt="image-20210104175456610" style="zoom:50%;" />



## 找不到包

main函数执行报错 找不到包，但是已经导入依赖了，可能是因为IDEA2020兼容性问题，使用Junit测试或者更换IDEA版本