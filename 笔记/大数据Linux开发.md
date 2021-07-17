# Linux

#### 1. 虚拟机Vmware上安装一个 Centos 系统（账号root密码a123456）

 <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210225225543512.png" alt="image-20210225225543512" style="zoom:50%;" />

#### 2. 虚拟机的三种网络配置方式的说明 张三（桥接）韩老师（Nat）

 <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210225225616385.png" alt="image-20210225225616385" style="zoom:50%;" />

#### 3. 双击进入虚拟机界面，ctrl+alt退出

#### 4. 上网

 <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210226004752992.png" alt="image-20210226004752992" style="zoom:50%;" />

#### 5. Linux目录结构

 <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210226005824891.png" alt="image-20210226005824891" style="zoom:70%;" />

 <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210226005851481.png" alt="image-20210226005851481" style="zoom:50%;" />

 <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210226005945328.png" alt="image-20210226005945328" style="zoom:50%;" />

 <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210226010001837.png" alt="image-20210226010001837" style="zoom:50%;" />

- 在 linux 中，目录结构 有一个 根目录 / ,其他的目录都是在 / 目录分支。
- 在 linux 中，有很多目录，是安装后，有自动有目录，每个目录都会存放相应的内容，不要去修改.

- 在 linux 中，所有的设备都可以通过文件来体现(字符设备文件[比如键盘，鼠标]，块设备文件[硬盘])

- 在学习 linux 时，要尽快的在脑海中，形成一个目录树

#### 6. vi和vim的三种模式的切换

  <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210226202755232.png" alt="image-20210226202755232" style="zoom:50%;" />

#### 7. vim快捷键

 <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210226203857920.png" alt="image-20210226203857920" style="zoom:67%;" />

#### 8. 远程操作开机重启

- 关机 &重启命令 重启命令
  - shutdown -h now  [ 立刻关机 ]
  - shutdown -h [ 1分钟后 ，关机 ]
  - shutdown -r now  [立刻重启 ]
  - shutdown -r 2 2 分钟后，重启
  - halt【立刻关机】
  - reboot【立刻重启】
  - 在重启和关机前，通常需要先执行
  - sync [把内存的数据，写入磁盘 把内存的数据，写入磁盘 ]
- 注意细节 :
  - 不管是重启系统还关闭，首先要运行 sync 命令，把内存中
    的数据写到磁盘
-  用户的登录和注销：
  - 登录时 尽量少用 root 帐号登录 ，因为它是系统管理员最大的权限避免操作失误。可以利用 ，因为它是系统管理员最大的权限避免操作失误。可以利用 普通用户登录，后再” su - 用户名’命令来切换成系统管理 员身份 .
  - 在提示符下输入 logout 即可注销用户【不同的 shell 可能不同 (logout exit) 】

#### 9. 用户管理

 <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210302010008069.png" alt="image-20210302010008069" style="zoom:70%;" />

- 指令
  - useradd xm 										自动创建/home/xm 目录
  - useradd -d /home/tiger/ xh 				将/home/tiger 作为家目录
  - passwd xm				设置密码
  - userdel xm 				删除用户，保留家目录（推荐，保存工作文件）
  - userdel -r xh 				删除用户，删除家目录
  - id xh 				查询用户，结果为（uid=501(xh) gid=501(xh) 组=501(xh)）
  - su - xm 				切换用户名
  - exit 				切换用户名后返回原来用户
  - whoami 				表示当前用户名
  - groupadd wudang 				创建组
  - groupdel wudang 				删除组
  - useradd -g wudang zwj 				创建用户到指定组
  - usermod -g shaolin zwj 				修改用户到另一指定组

- /etc/passwd 文件 用户（ user ）的配置文件，记录用户各种信息

- /etc/group 文件 组(group)的配置文件，记录的配置文件，记录 Linux 包含的组信息

#### 10. 运行级别

- 运行级别说明
    - 0 ：关机
    - 1 ：单用户 [类似安全模式， 这个模式可以帮助找回 root 密码]
    - 2：多用户状态没有网络服务
    - 3：多用户状态有网络服务 [**使用最多**]
    - 4：系统未使用保留给用户
    - 5：图形界面 【】
    - 6：系统重启 
    
    - 查看运行级别 vim /etc/inittab   id:5:initdefault 
    - init 1/2/3/4/5 可以修改运行级别

- 面试题，如何找回root密码
  - 启动时->快速输入 enter->输入 e-> 进入到编辑界面-> 选择中间有 kernel 项->
    输入 e(edit)-> 在该行的最后写入 1 [表示修改内核，临时生效]-> 输入 enter->
    输入 b [boot]-> 进入到单用模式 ->passwd修改密码->reboot重启

#### 11. 帮助指令

- man [命令或配置文件]（功能描述：获得帮助信息） 例如 man ls 即获取ls的用法

- help 命令 （功能描述：获得 shell 内置命令的帮助信息）例如 help cd 查看cd的用法

#### 12. 文件和目录相关的指令

- pwd (功能描述：显示当前工作目录的绝对路径 )
- ls [选项 ] [目录或是文件 ]
  - -a ：显示当前目录所有的 文件和，包括隐藏(文件名以 .开头就是隐藏 )。
  - -l ：以列表的方式显示信息
  - -h : 显示文件大小时，以 k , m, G单位显示
  - ls -alh 显示当前的目录内容
    ls -alh 指定目录   例子：ls -alh /root/ 

- cd  [参数 ] (功能描述：切换到指定目录 )
  - 使用绝对路径切换到 root 目录 cd /root
  - 使用相对路径到 /root 目录 cd ../root
  - 表示回到当前目录 的上一级cd ..
  - 回到家目录 cd

- mkdir指令 [make directory] 
  - 创建一个目录 例如 mkdir /home/dog
  - 创建多级目录 例如 mkdir -p /home/animal/cat

- rmdir 指令 [remove directory] 
  - 例如 rmdir /home/dog
  - 提示：如果需要删除非空目录，使用 rm -rf  例如 rm -rf /home/animal/cat

- touch 指令创建空文件， 指令创建空文件 例如 touch hello.txt
- cp 指拷贝文件到指定目录
  - cp [ 选项 ] source 【源】 dest 【目的文件】例如：cp aaa.txt bbb/
  - 递归复制整个文件夹 举例将 test目录的文件复制到zwj文件夹下: cp -r test/ zuj/
  - 强制覆盖不提示的方法： \cp -r test/ zuj/

- rm 指令移除文件或目录 例如rm aaa.txt
  - -r ：递归删除整个文件夹 rm -r /home/dog
  - -f ： 强制删除不提示
- mv 移动文件与目录或重命名
  - mv oldNameFile newNameFile 重另名 例子 mv aaa.txt bbb.txt
  - mv moveFile targetFolder/ 移动文件 例子 mv aaa.txt bbb/

- cat 查看文件内容(只能读) 例如 cat -n /etc/profile|more (进入之后enter下一行，空格下一页，q退出)
- more指令是一个基于 指令是一个基于 VI 编辑器的文本过滤，它以全屏幕方式按页显示件内容。
  - 例如 more /etc/profile 存在很多快捷键查看文档 例如enter 空格 q ctrl+f ctrl+b = :f 自行查阅

- less指令用来分屏查看文件内容，它的功能与 指令用来分屏查看文件内容，它的功能与 more指令类似，但是比 more more指令更加强大，支持各 指令更加强大，支持各 种显示终端。

- 重定向> 追加>>  例如：ls -l > a.txt 或 ls -l >> a.txt 将列表的内容写入到文件a.txt去（>覆盖 >>追加）
- echo指令输出内容到控制台 例如 echo $PATH
- head 用于显示文件的开头部分内容，默认情况下 head 指令显示文件的前 10 行 例如 head -n 10 /etc/profile
- tail 用于输出文件中尾部的内容  特殊：tail -f /etc/profile 实时记录文档所有更新，如果有变化会看到
- ln -s [ 原文件或目录] [ 软链接名 软链接名 ]  例如： ln -s /home/  linkToRoot  以后cd linkToRoot 可以进入home
- history（功能描述：查看已经执行过历史命令）例如：history 5 显示最近使用过的5个指令

#### 13.  时间日期类指令

- date指令 显示当前日期 
  - date（功能描述：显示当前时间）
  - date +%Y 显示年份 date "+%Y-%m -%d %H:%M:%S"  (+%Y固定，其他格式可以随意设置)
  - date s 字符串时间（功能描述：修改当前时间）例如：date -s "2021-03-06 18:25:40"

- cal [ 选项 ] （功能描述：不加选项，显示本月日历）例如：cal 2021

#### 14. 搜索查找类指令

- find [搜索范围 ] [选项 ]

  - -name<查找方式> 按照指定的文件名查找文件 例如：find /home -name hello.txt 
  - 根据文件格式查找 例如：find /home -name *.txt 

  - -user<用户名> 查找属于指定用户的文件 例如 find /home -user root
  - -size<文件大小> 按照指定的文件大小查找文件 例如：find /home -size +20M (+ - =)

- locate指令可以快速定位文件路径 例如：locate aaa.txt （查找前必须updatedb）
- grep 过滤查找 ， 管道符，“ |”，表示将前一个命令的处理结果输出传递给后面。例如：cat hello.txt | grep -n yes

#### 15. 压缩和解压缩类指令

- gzip/gunzip指令
  - gzip 文件 （功能描述：压缩文件，只将为 *.gz 文件）例如 gzip hello.txt (不会保留原文件)
  - gunzip 文件 .gz （功能描述：解压缩文件命令）gunzip hello.txt.gz
- zip/unzip指令
  - zip [选项 ] XXX.zip需要压缩的内容（功能描述：文件和目录命令）
    - -r：递归压缩，即目录 例如：zip -r mypackage.zip /home/
  - unzip [ 选项 ] XXX.zip（功能描述：解压缩文件） 
    - -d< 目录 > ：指定解压后文件的存放目录 例如 unzip -d /home/tmp/  mypackage.zip

- tar [选项 ] XXX.tar.gz  打包的内容 /目录 (功能描述：打包目录，压缩后的文件格式 .tar.gz)
  - -c 产生.tar打包文件 -v 显示详细信息 -f 指定压缩后的文件名 -z 打包同时压缩 -x 解压.tar文件
  - 压缩多个文件 例如: tar -zcvf a.tar.gz a1.txt a2.txt 压缩a1.txt和a2.txt为a.tar.gz
  - 打包整个home文件夹 例如: tar -zcvf myhome.tar.gz /home/
  - 解压到当前目录 例如：tar -zcvf a.tar.gz
  - 解压到某一目录中 例如：tar -zcvf myhome.tar.gz -C /home/  （目录要提前存在）

#### 16. 组管理和权限组

- 在 linux中的每个用户必须属于一个组，不能独立于组外。在 linux 中每个文件有所有者、 所在组，其他组的概念。

  文件的创建者为所有者，所有者所在的组为所在组，除了这个组之外则为其他组。

- ls -ahl  查看文件的所有者  例如：创建一个组 police,  再创建一个用户 tom,使用tom 来创建一个 文件 ok.txt

  即  groupadd police    |         useradd -g police tom            |        touch ok.txt       |         ls -ahl

- chown 【用户名】【文件名】  改变文件所有者  例如：chown tom apple.txt  （将所有者是root的apple.txt变为tom）

- chgrp  【组名】【文件名】改变文件所在的组 例如：chgrp police apple.txt

- usermod -g 【组名】【用户名】改变用户所在的组 例如：usermod -g bandit tom

- 权限的基本介绍

 <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210306223729711.png" alt="image-20210306223729711" style="zoom:70%;" />

 <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210306230838484.png" alt="image-20210306230838484" style="zoom:70%;" />

- 通过chmod指令，可以修改文件或者目录的权限
  - u:所有者	g:所有组	o:其他人	a:所有人（u、g、o的总和）
  - chmod u=rwx,g=rx，o=x   文件、目录【表示：给所有者rwx，给所在组的用户rx，给其他人x】
  - chmod o+w 文件、目录【表示：给其他用户增加w的权限】
  - chmod a-x 文件、目录【表示：给所有用户 去掉x权限】例如：chmod a-x abc.txt
  
  - 第二种方式通过数字变更权限 r=4 w=2 x=1  rwx=4+2+1=7 例如：chmod 751 abc.txt等同于chmod u=rwx,g=rx,o=x,abc.txt

- chown newowner:newgroup file 修改文件所有者和所在组 例如：chown tom:police kkk/  将kkk文件夹的文件修改 
- -R递归文件夹下所有文件的操作 例如：chown -R tom:police kkk/  将kkk文件夹的文件修改所有者和所在组为tom和police

#### 17. 定时任务调度

- crontab 进行 定时任务的调度
- 例如： 每隔1分钟，执行ls -l /etc/>>/tmp/to.txt
- 1.crontab -e 2.*/1 * * * * ls -l /etc/>>/tmp/to.txt  3.当保存退出后就程序 4.在每一分钟都会自动的调用ls -l /etc/>>/temp/to.txt

 <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210309012228789.png" alt="image-20210309012228789" style="zoom: 70%;" />

 <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210309012625697.png" alt="image-20210309012625697" style="zoom:50%;" />

- 案例(利用脚本)：每隔一分钟，将当前的日期信息，追加到/tmp/mydate文件中
  - 先编写一个文件/home/mytask1.sh     即date>>/tmp/mydate
  - 给mytask1.sh一个可执行权限     即chmod 744 /home/mytask1.sh
  - crontab -e
  - */1 * * * * /home/mytask1.sh

- crontab -r 终止任务调度   crontab -l  列出当前有哪些任务调度    service crond restart重启任务调度

#### 18. Linux磁盘分区、挂载

- Linux 来说无论有几个分区，分给哪一目录使用，它归根结底就只有一个根目录，一个独立且唯一的文件结构。
-  Linux 采用了一种叫“ 载入 (mount) ”的处理方法，它整个文件系统中包含了一套的文件和目录，且将一个分区和一个目录联系起来。这时要载入的一个分区使它存储空间在一个目录下获得。

 <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210309232327612.png" alt="image-20210309232327612" style="zoom:70%;" />

- 硬盘说明

 <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210309232755267.png" alt="image-20210309232755267" style="zoom:50%;" />

- 命令 ：lsblk -f 查看所有设备 (光驱 /media /media ，u盘， 硬盘 )挂载情况

 <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210309232653891.png" alt="image-20210309232653891" style="zoom:70%;" />

- 案例：增加一块硬盘
  - 增加一块硬盘 1G 
  - 重启一下服务器 然后lsblk -f 出现sdb
  - 给sdb硬盘分区 分区命令fdisk /dev/sdb
  - 格式化sdb1   mdfs -t ext4 /dev/sdb1
  - 挂载mkdir /home/newdisk      mount /dev/sdb1 /home/mydisk
  - 上面的方式， 只是临时生效当你重启系统挂载关没有配置 linux 的分区表，实现启 的分区表，实现启动时自动挂载 .
    vim /etc/fstab
  - 如果，我们希望卸载 指令：umount /dev/sdb1

- 查询系统整体磁盘使用情况 指令：df -lh
- 查询指定目录的磁盘占用情况 指令：du -h /目录 
- -s 指定目录占用大小汇总
  -h 带计量单位
  -a 含文件 含文件
  -- max -depth=1 子目录深度 子目录深度
  -c 列出明细的同时，增加汇总值 列
- 统计文件或文件夹个数

 <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210310000804162.png" alt="image-20210310000804162" style="zoom:60%;" />

#### 19. Linux的网络配置

- 自动获取IP地址   菜单栏 ‘系统’ 首选项 网路连接 点击编辑 缺点为每次ip地址分配不一样，不适合做服务器

 <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210310013500149.png" alt="image-20210310013500149" style="zoom:50%;" />

- 指定固定的ip地址 vim /etc/sysconfig/network-scripts/ifcfg-eth0 

- 配置生效方法：reboot或者service network restart

 <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210310020701242.png" alt="image-20210310020701242" style="zoom:70%;" />

- 修改主机名
  - 查看当前主机名 命令：hostname
  - 修改linux的主机映射文件vim letclsysconfig/network文件中内容
  - NETWORKING=yes NETWORKING_IPV6=no
  - HOSTNAME= hadoop 1 写入新的主机名注意:主机名称不要有“_”下划线3)修改/etc/hosts增加ip和主机的映射
    192.168.102.130 hadoop
  - 如果希望windows也可以通过主机名来连接centos，进入c: Windows\System32\driverslete\hosts 追加192.168.102.130 hadoop

#### 20. 进程管理（重点）

- 基本介绍

 <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210311000626447.png" alt="image-20210311000626447" style="zoom:60%;" />

- 显示系统执行的进程 ps 查看进行使用的指令 一般来说使用的参数是ps -aux

 <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210311000906463.png" alt="image-20210311000906463" style="zoom:60%;" />

 <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210311001241036.png" alt="image-20210311001241036" style="zoom:60%;" />

- ps -aux | grep sshd 查看有没有 sshd 进程

- ps -ef 查看父进程 ppid即为其进程父进程
- 终止进程 kill 和 killall
- kill [选项 ] 进程号（功能描述：通过进程号杀死进程）例如：先ps -aux | grep sshd 查看进程号后 kill 1869
- killall 进程名称 （功能描述：通过进程名称杀死，也支持配符，这在系统因负载大而变得很慢时很有用 例如：killall gedit

 <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210311002406930.png" alt="image-20210311002406930" style="zoom:60%;" />

- pstree  [选项 ] , 可以更加直观的来看进程信息 -u：显示进程的PID   -u：显示进程的所属用户
- 服务管理 服务 (service)本质就是进程，但运行在后台的，通常都会监听某个端口，等待其它程序的请求，比如 (mysql , sshd 防火墙等 )，因此我们又称为守护进程，是Linux 中非常重要的知识点。
- service 管理指令：service 服务名  [start | stop| restart| reload | status]  在 CentOS7.0 后 不再使用 service,而是 systemctl 
- 查看系统有哪些服务 setup 后系统服务可以查看  或者  /etc/init.d/服务名称

 <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210311005014240.png" alt="image-20210311005014240" style="zoom:40%;" />

- 给服务设置自启动/关闭

 <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210311005320278.png" alt="image-20210311005320278" style="zoom:50%;" />

- 动态监控进程 top [选项]

 <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210311005823131.png" alt="image-20210311005823131" style="zoom:80%;" />

 <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210311010137462.png" alt="image-20210311010137462" style="zoom:60%;" />

- top [选项]  选项有 P 以CPU使用率排序 M以内存使用率排序 N以PID排序 q退出top

- 查看系统网络情况netstat   指令netstat [ 选项 ]
  - -an an 按一定顺序排列输出
  - -p p 显示哪个进程在调用
  - 案例 请查看服务名为 sshd 的服务信息。netstat –anp | grep sshd 

#### 21. rpm和yum软件安装

- rpm 介绍 一种用于互联网下载包的打及安装工具，它含在某些 Linux 分发版中。它生成具有 .RPM 扩展名的文件

 <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210311011614523.png" alt="image-20210311011614523" style="zoom:50%;" />

- 卸载rpm软件包 指令：rpm -e RPM包的名称  例如 rpm -e firefox (rpm -e --nodeps foo强制删除，不推荐，依赖软件无法执行)

- 安装rpm软件包 指令：rpm -ivh RPM包全路径名称   rpm包在centos安装的镜像文件中，从/media/中找到rpm -ivh <路径>
- yum

 <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210311012918283.png" alt="image-20210311012918283" style="zoom:60%;" />

#### 22. 面试题

- 百度面试题：问题： Linux 常用命令，至少 6个(netstat , top lsblk find ps chkconfig )

- 瓜子二手车题 :

  问题： Linux 查看内存、 磁盘存储、io 读写、端口占用、进程等命令
  				top 						df –lh 	 iotop	  Netstat –tunlp	Ps -aux | grep 进程名

##### 小技巧： 使用上下键可以快速显示之前使用过的指令

##### 小技巧： ctrl+l 快速清空所以命令行

##### 小技巧： 让普通用户可以执行管理员指令 sudo 未在sudoers名单中需要管理员修改/etc/sudoers文件

##### 小技巧： 快速获得虚拟机ip地址 ifconfig eth0 | grep "inet addr" | cut -d ":" -f 2 | cut -d " " -f 1 

### 编辑文本常用快捷键

1、插入命令

　　　　a　　在光标后附加文本

　　　　A　　在本行行末附件文本

　　　　i　　在光标前插入文本

　　　　I　　在本行开始插入文本

　　　　o　　在光标下插入文本

　　　　O　　在光标上插入文本

 2、定位命令

　　　　h或方向左键　　左移一个字符

　　　　j或方向下键　　下移一行

　　　　k或方向上键　　上移一行

　　　　l或方向右键　　右移一个字符

　　　　$　　移至行尾

　　　　0　　移至行首

　　　　H　　移至屏幕上端

　　　　M　　移至屏幕中央

　　　　L　　移至屏幕下端

　　　　：set nu　　设置行号

　　　　：set nonu　　取消行号

　　　　gg　　到第一行

　　　　G　　到最后一行

　　　　nG　　到第n行

　　　　：n　　到第n行

　　3、删除命令

　　　　x　　删除光标所在的字符

　　　　nx　　删除光标所在处后n个字符

　　　　dd　　删除光标所在行

　　　　dG　　删除光标所在行导末尾的内容

　　　　D　　删除从光标所在行到末尾的内容

　　　　：n1,n2d　　删除指定范围的行

　　4、复制和剪切命令

　　　　yy或者Y　　复制当前行

　　　　nyy或者nY　　复制当前行一下n行

　　　　dd　　剪切当前行

　　　　ndd　　剪切当前行以下n行

　　　　p或者P　　粘贴在当前光标所在行下或行上

　　5、替换和取消命令

　　　　r　　取代光标所在处的字符

　　　　R　　从光标所在处开始替换字符，按Esc键结束

　　　　u　　取消上一步操作

　　6、**搜索和替换命令**

　　　　/string　　向前搜索指定命令，搜索时忽略大小写：**set ic** ，如搜索ftp:　　**/ftp**

　　　　**n**　　搜索指定字符串的下一个出现位置

　　　　:%s/被替换的字符串/替换成的字符创/g　　例如把全文的ftp替换成hello　　:%s/ftp/hello/g

　　　　:n1,n2s/被替换的字符串/替换成的字符创/g　　在一定的范围内替换字符串

　　7、保存退出命令

　　　　:wq 或者 ZZ　保存退出

　　　　:q!　　不保存退出

　　　　:wq!　　强行保存退出 （root或者文件的所有者）

　　8、应用实例

　　　　1）在vi中导入文件 ：

　　　　　　**:r 文件**

　　　　　　如：导入/test目录下的abc.txt文件　　:r /test/abc.txt

　　　　2）在vi中执行命令：

　　　　　　**:!命令**

　　　　　　如：在vi编辑器中查看 /test目录下的文件详细信息　　:!ls -l /test

　　　　　　这样就不需要先退出vi再去执行命令

　　　　3）把命令执行的结果导入到vi中：

　　　　　　如：把date命令执行的结果导入到vi中

　　　　　　**:r　!date**

　　　　4）定义快捷键

　　　　　　:map 快捷键 触发的命令

　　　　　　如：　　:map ^P I#<ESC>　　^表示定义的快捷键，

　　　　　　　　　　注意，在vi中，^这个符号是使用ctrl+v组合输入的符号，^P 在vi中的输入是ctrl+v和ctrl+p，或者ctrl+v+p

　　　　　　　　　　:map ^P I#<ESC>　　表示光标无论在行的哪个位置，只要按下ctrl+p就可以在行首插入#号，然后回到命令模式。I表示在行首插入文本，<ESC>表示　　回到命令模式。

　　　　　　　　　　:map ^E acodeartisan@gmail.com　　定义了一个快捷键ctrl+e，只要按下ctrl+e就可以在光标位置输入我的邮箱acodeartisan@gmail.com，非常方便。

　　　　　　　　　　再比如我们要定义一个快捷键，无论光标在行的哪个位置，只要按下ctrl+b，就可以去掉行首的#注释（实际上就是把行首的第一个字符删掉）

　　　　　　　　　　:map ^B 0x　　定义了一个快捷键ctrl+b。0表示移到行首，x表示删除光标所在字符。

　　　　5）连续行注释

　　　　　　:n1,n2s/^/#/g　　在n1行和n2行之间的行首加入#注释

　　　　　　:n1,n2s/^#//g　　把n1行和n2行之间的行首的#注释去掉

