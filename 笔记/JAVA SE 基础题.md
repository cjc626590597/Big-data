# JAVA SE 基础题

## Java程序初始化顺序：

1. 父类的静态代码块
2. 子类的静态代码块
3. 父类的普通代码块
4. 父类的构造方法
5. 子类的普通代码块
6. 子类的构造方法

<img src="https://uploadfiles.nowcoder.com/images/20191010/242025553_1570678660647_2A8BFCA92E7F024DFD2F7B0EA602002E" alt="img" style="zoom:80%;" />

## 1.java的内存模型

java 内存模型规定了所有的变量都存储在主内存中，但是每个线程会有自己的工作内存，线程的工作内存保存了该线程中使用了的变量（从主内存中拷贝的），线程对变量的操作都必须在工作内存中进行，不同线程之间无法直接访问对方工作内存中的变量，线程间变量值从传递都要经过主内存完成

![图片说明](https://uploadfiles.nowcoder.com/images/20180827/9780880_1535335958506_5721C0ED3C89479FA5F09D1F8E722D00)

##  

## 2.什么是原子性

一个操作是不可中断的，要么全部执行成功要么全部执行失败，比如银行转账

## 3.什么是可见性

当多个线程访问同一变量时，一个线程修改了这个变量的值，其他线程就能够立即看到修改的值

## 4.什么是有序性

程序执行的顺序按照代码的先后顺序执行

```java
int a = 0; //1
int b = 2; //2
```

像这2句代码1会比2先执行，但是jvm在正真执行时不一定是1在2之前，这里涉及一个概念叫做指令重排，处理器为了提高程序运行效率，可能会对输入代码进行优化，它不保证程序中各个语句的执行先后顺序同代码中的顺序一致，但是它会保证程序最终执行结果和代码顺序执行的结果是一致的。比如上面的代码语句1和语句2谁先执行对最终的程序结果并没有影响，那么就有可能在执行过程中，语句2先执行而语句1后执行。
在指令重排时会考虑指令之间的数据依赖性，比如2依赖了1的数值，那么处理器会保证1在2之前执行。
但是在多线程的情况下，指令重排就会有影响了。

## 5.volatile到底做了什么

- 禁止了指令重排
- 保证了不同线程对这个变量进行操作时的可见性，即一个线程修改了某个变量值，这个新值对其他线程是立即可见的
- 不保证原子性（线程不安全）

## 6. JVM内存结构

[一文搞懂JVM内存结构](https://blog.csdn.net/rongtaoup/article/details/89142396)



### 题目

#### 1. 有关List接口、Set接口和Map接口的描述，错误的是？ 

```
他们都继承自Collection接口
```

Collection
  -----List
​        -----LinkedList   非同步
​        ----ArrayList    非同步，实现了可变大小的元素数组
​        ----Vector      同步
​             ------Stack

  -----Set  不允许有相同的元素

Map
  -----HashTable    同步，实现一个key--value映射的哈希表
  -----HashMap      非同步，
  -----WeakHashMap  改进的HashMap，实现了“弱引用”，如果一个key不被引用，则被GC回收

#### 2. 关于Float，下列说法错误的是()

```
Float a=1.0是正确的赋值方法
```

1. Float是类，float不是类.
2. 查看JDK源码就可以发现Byte，Character，Short，Integer，Long，Float，Double，Boolean都在java.lang包中.
3. Float正确复制方式是Float f=1.0f,若不加f会被识别成double型,double无法向float隐式转换.
4. Float a= new Float(1.0)是正确的赋值方法，但是在1.5及以上版本引入自动装箱拆箱后，会提示这是不必要的装箱的警告，通常直接使用Float f=1.0f.

#### 3. 下面哪个标识符是合法的？

```
"Hello*World"
```

标识符是以字母开头的字母数字序列：
数字是指0~9，字母指大小写英文字母、下划线（_)和美元符号（$），也可以是Unicode字符集中的字符，如汉字；
字母、数字等字符的任意组合，不能包含+、- *等字符；

#### 4. What is the result of compiling and executing the following fragment of code:

```java
Boolean flag = false;
if (flag = true)
{
    System.out.println(“true”);
}
else
{
    System.out.println(“false”);
}
```

```
The text“true” is displayed.
```

Boolean修饰的变量为包装类型，初始化值为false,进行赋值时会调用Boolean.valueOf(boolean b)方法自动拆箱为基本数据类型，因此赋值后flag值为true，输出文本true。 如果使用==比较,则输出文本false。if的语句比较，除boolean外的其他类型都不能使用赋值语句，否则会提示无法转成布尔值。

#### 5. 构造函数不能被继承，构造方法只能被显式或隐式的调用。

#### 6. &&只要有false就停止，&所有运算就会执行

String s=null;没有给s开辟任何空间，当执行length()方法时候，
因为没有具体指向的内存空间，所以报出NullPointerException没有指向的错误。
A &是与，位运算，两个都得执行，执行到s.length()自然就报错了。
B S！=null 结果为false 整体就为false ，&& 后面就不会执行。下面的同理。

#### 7. Collection 接口常用的方法

1. size():返回集合中元素的个数
2. add(Object obj):向集合中添加一个元素
3. addAll(Colletion coll):将形参coll包含的所有元素添加到当前集合中
4. isEmpty():判断这个集合是否为空
5. clear():清空集合元素
6. contains(Object obj):判断集合中是否包含指定的obj元素
   ① 判断的依据：根据元素所在类的equals()方法进行判断
   ②明确：如果存入集合中的元素是自定义的类对象，要去：自定义类要重写equals()方法
7. constainsAll(Collection coll):判断当前集合中是否包含coll的所有元素
8. rentainAll(Collection coll):求当前集合与coll的共有集合，返回给当前集合
9. remove(Object obj):删除集合中obj元素，若删除成功，返回ture否则
10. removeAll(Collection coll):从当前集合中删除包含coll的元素
11. equals(Object obj):判断集合中的所有元素 是否相同
12. hashCode():返回集合的哈希值
13. toArray(T[] a):将集合转化为数组
    ①如有参数，返回数组的运行时类型与指定数组的运行时类型相同。
14. iterator():返回一个Iterator接口实现类的对象,进而实现集合的遍历。
15. 数组转换为集合：Arrays.asList(数组)

#### 8. Map

 ![img](https://uploadfiles.nowcoder.com/images/20190514/2970531_1557843217158_F6C79F5BE8BA80BAA8CB04BB40951DBB)

 ![img](https://uploadfiles.nowcoder.com/images/20190514/2970531_1557843285711_B37B82009A3A0D1283C42A49F05F8BC7)

#### 9. 访问权限修饰符

 ![img](http://uploadfiles.nowcoder.com/images/20150921/458054_1442766565525_E93E59ACFE1791E0A5503384BEBDC544)

（ 1 ）对于外部类而言，它也可以使用访问控制符修饰，但外部类只能有两种访问控制级别： public 和默认。因为外部类没有处于任何类的内部，也就没有其所在类的内部、所在类的子类两个范围，因此 private 和 protected 访问控制符对外部类没有意义。

（ 2 ）内部类的上一级程序单元是外部类，它具有 4 个作用域：同一个类（ private ）、同一个包（ protected ）和任何位置（ public ）。

（ 3 ） 因为局部成员的作用域是所在方法，其他程序单元永远不可能访问另一个方法中的局部变量，所以所有的局部成员都不能使用访问控制修饰符修饰。

#### 10.动态语言

动态语言的定义：动态编程语言  是  [高级程序设计语言](http://zh.wikipedia.org/wiki/高级程序设计语言)  的一个类别，在计算机科学领域已被广泛应用。它是一类  在 ***运行时可以改变其结构的语言\***  ：例如新的函数、对象、甚至代码可以被引进，已有的函数可以被删除或是其他结构上的变化。动态语言目前非常具有活力。众所周知的  [ECMAScript](http://zh.wikipedia.org/wiki/ECMAScript)  （  [JavaScript](http://zh.wikipedia.org/wiki/JavaScript)  ）便是一个动态语言，除此之外如  [PHP](http://zh.wikipedia.org/wiki/PHP)  、  [Ruby](http://zh.wikipedia.org/wiki/Ruby)  、  [Python](http://zh.wikipedia.org/wiki/Python)  等也都属于动态语言，而  [C](http://zh.wikipedia.org/wiki/C语言)  、  [C++](http://zh.wikipedia.org/wiki/C%2B%2B) 、JAVA 等语言则不属于动态语言。

#### 11.socket编程中，以下哪个socket的操作是不属于服务端操作的（）？

 ![img](https://uploadfiles.nowcoder.com/images/20180316/8955099_1521189690989_0BB28C2A1ECCC47EC020E89E8A554BBC)

#### 12 数值转换

数据类型的转换，分为自动转换和强制转换。自动转换是程序在执行过程中 “ 悄然 ” 进行的转换，不需要用户提前声明，一般是从位数低的类型向位数高的类型转换；强制类型转换则必须在代码中声明，转换顺序不受限制。

##### **自动数据类型转换**

自动转换按从低到高的顺序转换。不同类型数据间的优先关系如下：
低 ---------------------------------------------> 高
byte,short,char-> int -> long -> float -> double

##### **强制数据类型转换**

强制转换的格式是在需要转型的数据前加上 “( )” ，然后在括号内加入需要转化的数据类型。有的数据经过转型运算后，精度会丢失，而有的会更加精确

##### **java自动装箱**

自动装箱规则: Double Integer Long Float只接受自己基础类型的数据(double int long float)  Character Short Byte 都可以接受char int short byte四种基础数据类型直接封装

#### 13 垃圾回收

下面哪些描述是正确的：（ ）

```java
public` `class` `Test {
public` `static` `class` `A {
private` `B ref;
public` `void` `setB(B b) {
ref = b;
}
}
public` `static` `Class B {
private` `A ref;
public` `void` `setA(A a) {
ref = a;
}
}
public` `static` `void` `main(String args[]) {
…
start();
….
}
public` `static` `void` `start() { A a = ``new` `A();
B b = ``new` `B();
a.setB(b);
b = ``null``; ``//
a = ``null``;
…
}
}
```

 <img src="https://img-blog.csdnimg.cn/20210618135329942.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80NTQxMzk2NQ==,size_16,color_FFFFFF,t_70" style="zoom:80%;" />

 <img src="https://uploadfiles.nowcoder.com/images/20200408/165701207_1586336189484_802A6FE5D8D89EC50285B88F46C488F4" alt="img" style="zoom:60%;" />

#### 14. String s = new String("xyz");创建了几个StringObject？
1.String对象的两种创建方式:
第一种方式: String str1 = "aaa"; 是在常量池中获取对象("aaa" 属于字符串字面量，因此编译时期会在常量池中创建一个字符串对象)，
第二种方式: String str2 = new String("aaa") ; 一共会创建两个字符串对象一个在堆中，一个在常量池中（前提是常量池中还没有 "aaa" 字符串对象）。
System.out.println(str1==str2);//false

2.String类型的常量池比较特殊。它的主要使用方法有两种：
直接使用双引号声明出来的String对象会直接存储在常量池中。
如果不是用双引号声明的String对象,可以使用 String 提供的 intern 方法。 String.intern() 是一个 Native 方法，它的作用是： 如果运行时常量池中已经包含一个等于此 String 对象内容的字符串，则返回常量池中该字符串的引用； 如果没有，则在常量池中创建与此 String 内容相同的字符串，并返回常量池中创建的字符串的引用。
String s1 = new String("AAA");
String s2 = s1.intern();
String s3 = "AAA";
System.out.println(s2);//AAA
System.out.println(s1 == s2);//false，因为一个是堆内存中的String对象一个是常量池中的String对象，
System.out.println(s2 == s3);//true， s2,s3指向常量池中的”AAA“

#### 15.线程安全集合

在集合框架中，有些类是线程安全的，这些都是jdk1.1中的出现的。在jdk1.2之后，就出现许许多多非线程安全的类。 下面是这些线程安全的同步的类：

vector：就比arraylist多了个同步化机制（线程安全），因为效率较低，现在已经不太建议使用。在web应用中，特别是前台页面，往往效率（页面响应速度）是优先考虑的。

statck：堆栈类，先进后出

hashtable：就比hashmap多了个线程安全

enumeration：枚举，相当于迭代器

除了这些之外，其他的都是非线程安全的类和接口。

#### 16.**方法的重写（override）两同两小一大原则**：

方法名相同，参数类型相同

子类返回类型小于等于父类方法返回类型，

子类抛出异常小于等于父类方法抛出异常，

子类访问权限大于等于父类方法访问权限。

#### 17.客户端和服务端的创建方法

//创建Socket 客户端对象
Socket s = new Socket("[127.0.0.1](https://www.baidu.com/s?wd=127.0.0.1&tn=SE_PcZhidaonwhc_ngpagmjz&rsv_dl=gh_pc_zhidao)",6666);
//创建ServerSocket [服务器端](https://www.baidu.com/s?wd=服务器端&tn=SE_PcZhidaonwhc_ngpagmjz&rsv_dl=gh_pc_zhidao)对象。。
ServerSocket ss = new ServerSocket(6666);
//监听服务器连接
s = ss.accept();

#### 18. Daemon线程

java线程是一个运用很广泛的重点知识,我们很有必要了解java的daemon线程.

1.首先我们必须清楚的认识到java的线程分为两类: 用户线程和daemon线程

A. 用户线程: 用户线程可以简单的理解为用户定义的线程,当然包括main线程(以前我错误的认为main线程也是一个daemon线程,但是慢慢的发现原来main线程不是,因为如果我再main线程中创建一个用户线程,并且打出日志,我们会发现这样一个问题,main线程运行结束了,但是我们的线程任然在运行).

B. daemon线程: daemon线程是为我们创建的用户线程提供服务的线程,比如说jvm的GC等等,这样的线程有一个非常明显的特征: 当用户线程运行结束的时候,daemon线程将会自动退出.(由此我们可以推出下面关于daemon线程的几条基本特点)

2.daemon 线程的特点: 

A. 守护线程创建的过程中需要先调用setDaemon方法进行设置,然后再启动线程.否则会报出IllegalThreadStateException异常.(个人在想一个问题,为什么不能动态更改线程为daemon线程?有时间一个补上这个内容,现在给出一个猜测: 是因为jvm判断线程状态的时候,如果当前只存在一个线程Thread1,如果我们把这个线程动态更改为daemon线程,jvm会认为当前已经不存在用户线程而退出,稍后将会给出正确结论,抱歉!如果有哪位大牛看到,希望给出指点,谢谢!)

B. 由于daemon线程的终止条件是当前是否存在用户线程,所以我们不能指派daemon线程来进行一些业务操作,而只能服务用户线程.

C. daemon线程创建的子线程任然是daemon线程.

#### 19. 线程方法

1.sleep()方法

在指定时间内让当前正在执行的线程暂停执行，但不会释放“锁标志”。不推荐使用。

sleep()使当前线程进入阻塞状态，在指定时间内不会执行。

2.wait()方法

在其他线程调用对象的notify或notifyAll方法前，导致当前线程等待。线程会释放掉它所占有的“锁标志”，从而使别的线程有机会抢占该锁。

当前线程必须拥有当前对象锁。如果当前线程不是此锁的拥有者，会抛出IllegalMonitorStateException异常。

唤醒当前对象锁的等待线程使用notify或notifyAll方法，也必须拥有相同的对象锁，否则也会抛出IllegalMonitorStateException异常。

waite()和notify()必须在synchronized函数或synchronized　block中进行调用。如果在non-synchronized函数或non-synchronized　block中进行调用，虽然能编译通过，但在运行时会发生IllegalMonitorStateException的异常。

3.yield方法 

暂停当前正在执行的线程对象。

yield()只是使当前线程重新回到可执行状态，所以执行yield()的线程有可能在进入到可执行状态后马上又被执行。

yield()只能使同优先级或更高优先级的线程有执行的机会。 

4.join方法

等待该线程终止。

等待调用join方法的线程结束，再继续执行。如：t.join();//主要用于等待t线程运行结束，若无此句，main则会执行完毕，导致结果不可预测。

#### 20. 排序复杂度

 ![在这里插入图片描述](https://img-blog.csdnimg.cn/20210701102801130.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80NTQxMzk2NQ==,size_16,color_FFFFFF,t_70)

#### 21. Hibernate 

- 、什么是延迟加载？
  - 定义：延迟加载(lazy load)是Hibernate3 关联关系对象默认的加载方式，延迟加载机制是为了避免一些无谓的性能开销而提出来的。**就是只有当真正需要数据时，才真正的执行数据加载操作。**延迟加载是 hibernate 中用于提高查询效率的一种措施，它的对立面是 立即加载。
- 2、如何实现延迟加载？
  - Hibernate 2 实现延迟加载有 2 种方式：
    - 实体对象
    - 集合
  - Hibernate 3 又引入了一种新的加载方式：属性的延迟加载
  - 一般使用load()的方法来实现延迟加载：
    - 当调用load方法加载对象时，返回***对象，等到真正用到对象的内容时才发出sql语句
- 3、Hibernate 其他介绍
  - Hibernate 使用 Java 反射机制，而不是字节码增强程序来实现透明性
  - Hibernate 的性能非常好，因为它是个轻量级框架。映射的灵活性很出色。它支持各种关系数据库，从一对一到多对多的各种复杂关系。
- 4、优化 Hibernate 所鼓励的 7 大措施
  - 1.尽量使用多对一，避免使用单项一对多
  - 2.灵活使用单向一对多
  - 3.不用一对一，使用多对一代替一对一
  - 4.配置对象缓存，不使用集合缓存
  - 5.一对多使用Bag ，多对一使用Set
  - 6.继承使用显示多态 HQL:from object polymorphism="exlicit" 避免查处所有对象
  - 7.消除大表，使用二级缓存

#### 22. **四种引用类型**

JDK1.2 之前，一个对象只有“已被引用”和"未被引用"两种状态，这将无法描述某些特殊情况下的对象，比如，当内存充足时需要保留，而内存紧张时才需要被抛弃的一类对象。

所以在 JDK.1.2 之后，Java 对引用的概念进行了扩充，将引用分为了：强引用（Strong Reference）、软引用（Soft Reference）、弱引用（Weak Reference）、虚引用（Phantom Reference）4 种，这 4 种引用的强度依次减弱。

**一，强引用**

Object obj = new Object(); //只要obj还指向Object对象，Object对象就不会被回收 obj = null; //手动置null

只要强引用存在，垃圾回收器将永远不会回收被引用的对象，哪怕内存不足时，JVM也会直接抛出OutOfMemoryError，不会去回收。如果想中断强引用与对象之间的联系，可以显示的将强引用赋值为null，这样一来，JVM就可以适时的回收对象了

**二，软引用**

软引用是用来描述一些非必需但仍有用的对象。**在内存足够的时候，软引用对象不会被回收，只有在内存不足时，系统则会回收软引用对象，如果回收了软引用对象之后仍然没有足够的内存，才会抛出内存溢出异常**。这种特性常常被用来实现缓存技术，比如网页缓存，图片缓存等。

在 JDK1.2 之后，用java.lang.ref.SoftReference类来表示软引用。

**三，弱引用**

弱引用的引用强度比软引用要更弱一些，**无论内存是否足够，只要 JVM 开始进行垃圾回收，那些被弱引用关联的对象都会被回收**。在 JDK1.2 之后，用 java.lang.ref.WeakReference 来表示弱引用。

**四，虚引用**

虚引用是最弱的一种引用关系，如果一个对象仅持有虚引用，那么它就和没有任何引用一样，它随时可能会被回收，在 JDK1.2 之后，用 PhantomReference 类来表示，通过查看这个类的源码，发现它只有一个构造函数和一个 get() 方法，而且它的 get() 方法仅仅是返回一个null，也就是说将永远无法通过虚引用来获取对象，虚引用必须要和 ReferenceQueue 引用队列一起使用。

#### 23. hashCode()方法与equals()方法

hashCode()方法和equals()方法的作用其实是一样的，在Java里都是用来对比两个对象是否相等一致。

那么equals()既然已经能实现对比的功能了，为什么还要hashCode()呢？因为重写的equals()里一般比较的比较全面比较复杂，这样效率就比较低，而利用hashCode()进行对比，则只要生成一个hash值进行比较就可以了，效率很高。

那么hashCode()既然效率这么高为什么还要equals()呢？因为hashCode()并不是完全可靠，有时候不同的对象他们生成的hashcode也会一样（生成hash值得公式可能存在的问题），所以hashCode()只能说是大部分时候可靠，并不是绝对可靠，

所以我们可以得出：

1.equals()相等的两个对象他们的hashCode()肯定相等，也就是用equals()对比是绝对可靠的。

2.hashCode()相等的两个对象他们的equal()不一定相等，也就是hashCode()不是绝对可靠的。

所有对于需要大量并且快速的对比的话如果都用equals()去做显然效率太低，所以解决方式是，每当需要对比的时候，首先用hashCode()去对比，如果hashCode()不一样，则表示这两个对象肯定不相等（也就是不必再用equal()去再对比了）,如果hashCode()相同，此时再对比他们的equals()，如果equals()也相同，则表示这两个对象是真的相同了，这样既能大大提高了效率也保证了对比的绝对正确性！

#### 24. JVM配置参数

Xms 起始内存

Xmx 最大内存

Xmn 新生代内存

Xss 栈大小。 就是创建线程后，分配给每一个线程的内存大小

-XX:NewRatio=n:设置年轻代和年老代的比值。如:为3，表示年轻代与年老代比值为1：3，年轻代占整个年轻代年老代和的1/4

-XX:SurvivorRatio=n:年轻代中Eden区与两个Survivor区的比值。注意Survivor区有两个。如：3，表示Eden：Survivor=3：2，一个Survivor区占整个年轻代的1/5

-XX:MaxPermSize=n:设置持久代大小

收集器设置
-XX:+UseSerialGC:设置串行收集器
-XX:+UseParallelGC:设置并行收集器
-XX:+UseParalledlOldGC:设置并行年老代收集器
-XX:+UseConcMarkSweepGC:设置并发收集器
垃圾回收统计信息
-XX:+PrintGC
-XX:+PrintGCDetails
-XX:+PrintGCTimeStamps
-Xloggc:filename
并行收集器设置
-XX:ParallelGCThreads=n:设置并行收集器收集时使用的CPU数。并行收集线程数。
-XX:MaxGCPauseMillis=n:设置并行收集最大暂停时间
-XX:GCTimeRatio=n:设置垃圾回收时间占程序运行时间的百分比。公式为1/(1+n)
并发收集器设置
-XX:+CMSIncrementalMode:设置为增量模式。适用于单CPU情况。
-XX:ParallelGCThreads=n:设置并发收集器年轻代收集方式为并行收集时，使用的CPU数。并行收集线程数。

#### 25. IO流继承关系

 <img src="http://uploadfiles.nowcoder.com/images/20150328/138512_1427527478646_1.png" alt="img" style="zoom:80%;" />

#### 26. 表达式转型规则

当使用 +、-、*、/、%、运算操作是，遵循如下规则：

1、所有的byte,short,char型的值将被提升为int型；

2、如果有一个操作数是long型，计算结果是long型；

3、如果有一个操作数是float型，计算结果是float型；

4、如果有一个操作数是double型，计算结果是double型；

5、被fianl修饰的变量不会自动改变类型，当2个final修饰相操作时，结果会根据左边变量的类型而转化。

#### 27. final的重要知识点;

1、final关键字可以用于成员变量、本地变量、方法以及类。

2、 final成员变量必须在声明的时候初始化或者在构造器中初始化，否则就会报编译错误。

3、 你不能够对final变量再次赋值。

4、 本地变量必须在声明时赋值。

5、 在匿名类中所有变量都必须是final变量。

6、 final方法不能被重写。

7、 final类不能被继承。

8、 没有在声明时初始化final变量的称为空白final变量(blank final variable)，它们必须在构造器中初始化，或者调用this()初始化。不这么做的话，编译器会报错“final变量(变量名)需要进行初始化”。

#### 28. 五个基本原则：

单一职责原则（Single-Resposibility Principle）：一个类，最好只做一件事，只有一个引起它的变化。单一职责原则可以看做是低耦合、高内聚在面向对象原则上的引申，将职责定义为引起变化的原因，以提高内聚性来减少引起变化的原因。
开放封闭原则（Open-Closed principle）：软件实体应该是可扩展的，而不可修改的。也就是，对扩展开放，对修改封闭的。
Liskov替换原则（Liskov-Substituion Principle）：子类必须能够替换其基类。这一思想体现为对继承机制的约束规范，只有子类能够替换基类时，才能保证系统在运行期内识别子类，这是保证继承复用的基础。
依赖倒置原则（Dependecy-Inversion Principle）：依赖于抽象。具体而言就是高层模块不依赖于底层模块，二者都同依赖于抽象；抽象不依赖于具体，具体依赖于抽象。
接口隔离原则（Interface-Segregation Principle）：使用多个小的专门的接口，而不要使用一个大的总接口

#### 29. 类与类的关系

**USES-A：**依赖关系，A类会用到B类，这种关系具有偶然性，临时性。但B类的变化会影响A类。这种在代码中的体现为：A类方法中的参数包含了B类。

**关联关系：**A类会用到B类，这是一种强依赖关系，是长期的并非偶然。在代码中的表现为：A类的成员变量中含有B类。

**HAS-A：**聚合关系，拥有关系，是**关联关系**的一种特例，是整体和部分的关系。比如鸟群和鸟的关系是聚合关系，鸟群中每个部分都是鸟。

**IS-A：**表示继承。父类与子类，这个就不解释了。

要注意：还有一种关系：**组合关系**也是关联关系的一种特例，它体现一种contains-a的关系，这种关系比聚合更强，也称为强聚合。它同样体现整体与部分的关系，但这种整体和部分是不可分割的。
