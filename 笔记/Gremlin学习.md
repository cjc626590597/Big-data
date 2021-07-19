# Gremlin学习

### 数据创建

##### 1. 创建属性类型（PropertyKey）

```groovy
graph.schema().propertyKey("name").asText().create() // 创建姓名属性，文本类型
graph.schema().propertyKey("age").asInt().create()   // 创建年龄属性，整数类型
graph.schema().propertyKey("addr").asText().create() // 创建地址属性，文本类型
graph.schema().propertyKey("lang").asText().create() // 创建语言属性，文本类型
graph.schema().propertyKey("tag").asText().create()  // 创建标签属性，文本类型
graph.schema().propertyKey("weight").asFloat().create() // 创建权重属性，浮点类型
```

##### 2. 创建顶点类型（VertexLabel）

```groovy
// 创建顶点类型：人"person"，包含姓名、年龄、地址等属性，使用自定义的字符串作为ID
graph.schema().vertexLabel("person")
              .properties("name", "age", "addr", "weight")
              .useCustomizeStringId()
              .create()
// 创建顶点类型：软件"software"，包含名称、使用语言、标签等属性，使用名称作为主键
graph.schema().vertexLabel("software")
              .properties("name", "lang", "tag", "weight")
              .primaryKeys("name")
              .create()
// 创建顶点类型：语言"language"，包含名称、使用语言等属性，使用名称作为主键
graph.schema().vertexLabel("language")
              .properties("name", "lang", "weight")
              .primaryKeys("name")
              .create()
```

##### 3. 创建边类型（EdgeLabel）

```groovy
// 创建边类型：人认识人"knows"，此类边由"person"指向"person"
graph.schema().edgeLabel("knows")
              .sourceLabel("person").targetLabel("person")
              .properties("weight")
              .create()
// 创建边类型：人创建软件"created"，此类边由"person"指向"software"
graph.schema().edgeLabel("created")
              .sourceLabel("person").targetLabel("software")
              .properties("weight")
              .create()
// 创建边类型：软件包含软件"contains"，此类边由"software"指向"software"
graph.schema().edgeLabel("contains")
              .sourceLabel("software").targetLabel("software")
              .properties("weight")
              .create()
// 创建边类型：软件定义语言"define"，此类边由"software"指向"language"
graph.schema().edgeLabel("define")
              .sourceLabel("software").targetLabel("language")
              .properties("weight")
              .create()
// 创建边类型：软件实现软件"implements"，此类边由"software"指向"software"
graph.schema().edgeLabel("implements")
              .sourceLabel("software").targetLabel("software")
              .properties("weight")
              .create()
// 创建边类型：软件支持语言"supports"，此类边由"software"指向"language"
graph.schema().edgeLabel("supports")
              .sourceLabel("software").targetLabel("language")
              .properties("weight")
              .create()
```

##### 4. 创建index类型（IndexLabel）

```groovy
graph.schema().indexLabel("personByAge").onV("person").by("age").range().ifNotExist().create()
graph.schema().indexLabel("personByAge").onV("person").by("age").range().ifNotExist().create()
```

##### 5. 添加顶点与边

```groovy
// 调用Gremlin的addVertex方法添加顶点，参数由顶点label、id、properties构成
// T.label表示顶点类型、T.id表示顶点id
// 后面接着的"name"、"age"等是顶点属性，每个属性由一对键值组成

// 添加2个作者顶点
okram = graph.addVertex(T.label, "person", T.id, "okram",
                        "name", "Marko A. Rodriguez", "age", 29,
                        "addr", "Santa Fe, New Mexico", "weight", 1)
spmallette = graph.addVertex(T.label, "person", T.id, "spmallette",
                             "name", "Stephen Mallette", "age", 0,
                             "addr", "", "weight", 1)

// 添加TinkerPop顶点
tinkerpop = graph.addVertex(T.label, "software", "name", "TinkerPop",
                            "lang", "java", "tag", "Graph computing framework",
                            "weight", 1)
// 添加TinkerGraph顶点
tinkergraph = graph.addVertex(T.label, "software", "name", "TinkerGraph",
                              "lang", "java", "tag", "In-memory property graph",
                              "weight", 1)
// 添加Gremlin顶点
gremlin = graph.addVertex(T.label, "language", "name", "Gremlin",
                          "lang", "groovy/python/javascript", "weight", 1)

// 调用Gremlin的addEdge方法添加边
// 由源顶点对象发起调用，参数由边类型、目标顶点、属性构成
// 后面接着的"name"、"age"等是顶点属性，每个属性由一对键值组成

// 添加2位作者创建TinkerPop的边
okram.addEdge("created", tinkerpop, "weight", 1)
spmallette.addEdge("created", tinkerpop, "weight", 1)

// 添加2位作者的认识边
okram.addEdge("knows", spmallette, "weight", 1)

// 添加TinkerPop、TinkerGraph、Gremlin之间的关系边
tinkerpop.addEdge("define", gremlin, "weight", 1)
tinkerpop.addEdge("contains", tinkergraph, "weight", 1)
tinkergraph.addEdge("supports", gremlin, "weight", 1)

// 注意：下面的Gremlin语句在执行时需要紧接着上述第一步的语句
// 因为这里使用了上面定义的tinkerpop、gremlin等变量

// 添加3个作者顶点
javeme = graph.addVertex(T.label, "person", T.id, "javeme",
                         "name", "Jermy Li", "age", 29, "addr",
                         "Beijing", "weight", 1)
zhoney = graph.addVertex(T.label, "person", T.id, "zhoney",
                         "name", "Zhoney Zhang", "age", 29,
                         "addr", "Beijing", "weight", 1)
linary = graph.addVertex(T.label, "person", T.id, "linary",
                         "name", "Linary Li", "age", 28,
                         "addr", "Wuhan. Hubei", "weight", 1)

// 添加HugeGraph顶点
hugegraph = graph.addVertex(T.label, "software", "name", "HugeGraph",
                            "lang", "java", "tag", "Graph Database",
                            "weight", 1)

// 添加作者创建HugeGraph的边
javeme.addEdge("created", hugegraph, "weight", 1)
zhoney.addEdge("created", hugegraph, "weight", 1)
linary.addEdge("created", hugegraph, "weight", 1)

// 添加作者之间的关系边
javeme.addEdge("knows", zhoney, "weight", 1)
javeme.addEdge("knows", linary, "weight", 1)

// 添加HugeGraph实现TinkerPop的边
hugegraph.addEdge("implements", tinkerpop, "weight", 1)
// 添加HugeGraph支持Gremlin的边
hugegraph.addEdge("supports", gremlin, "weight", 1)

// 注意：下面的Gremlin语句在执行时需要紧接着上述第一步的语句
// 因为这里使用了上面定义的tinkerpop、gremlin等变量

// 添加2个作者顶点
alaro = graph.addVertex(T.label, "person", T.id, "dalaro",
                        "name", "Dan LaRocque ", "age", 0,
                        "addr", "", "weight", 1)
mbroecheler = graph.addVertex(T.label, "person", T.id, "mbroecheler",
                              "name", "Matthias Broecheler",
                              "age", 0, "addr", "San Francisco", "weight", 1)

// 添加Titan顶点
titan = graph.addVertex(T.label, "software", "name", "Titan",
                        "lang", "java", "tag", "Graph Database", "weight", 1)

// 添加作者、Titan之间的关系边
dalaro.addEdge("created", titan, "weight", 1)
mbroecheler.addEdge("created", titan, "weight", 1)
okram.addEdge("created", titan, "weight", 1)

dalaro.addEdge("knows", mbroecheler, "weight", 1)

// 添加Titan与TinkerPop、Gremlin之间的关系边
titan.addEdge("implements", tinkerpop, "weight", 1)
titan.addEdge("supports", gremlin, "weight", 1)

// 查询所有的顶点"g.V()" (在HugeGraph-Studio中执行该语句时，顶点的关联边也会一道被查询出来)
// 或者也可使用查询所有边"g.E()"进行验证
g.V()
```

##### 6. 完整代码

```groovy
// PropertyKey
graph.schema().propertyKey("name").asText().create()
graph.schema().propertyKey("age").asInt().create()
graph.schema().propertyKey("addr").asText().create()
graph.schema().propertyKey("lang").asText().create()
graph.schema().propertyKey("tag").asText().create()
graph.schema().propertyKey("weight").asFloat().create()

// VertexLabel
graph.schema().vertexLabel("person").properties("name", "age", "addr", "weight").useCustomizeStringId().create()
graph.schema().vertexLabel("software").properties("name", "lang", "tag", "weight").primaryKeys("name").create()
graph.schema().vertexLabel("language").properties("name", "lang", "weight").primaryKeys("name").create()

// EdgeLabel
graph.schema().edgeLabel("knows").sourceLabel("person").targetLabel("person").properties("weight").create()
graph.schema().edgeLabel("created").sourceLabel("person").targetLabel("software").properties("weight").create()
graph.schema().edgeLabel("contains").sourceLabel("software").targetLabel("software").properties("weight").create()
graph.schema().edgeLabel("define").sourceLabel("software").targetLabel("language").properties("weight").create()
graph.schema().edgeLabel("implements").sourceLabel("software").targetLabel("software").properties("weight").create()
graph.schema().edgeLabel("supports").sourceLabel("software").targetLabel("language").properties("weight").create()


// TinkerPop
okram = graph.addVertex(T.label, "person", T.id, "okram", "name", "Marko A. Rodriguez", "age", 29, "addr", "Santa Fe, New Mexico", "weight", 1)
spmallette = graph.addVertex(T.label, "person", T.id, "spmallette", "name", "Stephen Mallette", "age", 0, "addr", "", "weight", 1)

tinkerpop = graph.addVertex(T.label, "software", "name", "TinkerPop", "lang", "java", "tag", "Graph computing framework", "weight", 1)
tinkergraph = graph.addVertex(T.label, "software", "name", "TinkerGraph", "lang", "java", "tag", "In-memory property graph", "weight", 1)
gremlin = graph.addVertex(T.label, "language", "name", "Gremlin", "lang", "groovy/python/javascript", "weight", 1)

okram.addEdge("created", tinkerpop, "weight", 1)
spmallette.addEdge("created", tinkerpop, "weight", 1)

okram.addEdge("knows", spmallette, "weight", 1)

tinkerpop.addEdge("define", gremlin, "weight", 1)
tinkerpop.addEdge("contains", tinkergraph, "weight", 1)
tinkergraph.addEdge("supports", gremlin, "weight", 1)

// Titan
dalaro = graph.addVertex(T.label, "person", T.id, "dalaro", "name", "Dan LaRocque ", "age", 0, "addr", "", "weight", 1)
mbroecheler = graph.addVertex(T.label, "person", T.id, "mbroecheler", "name", "Matthias Broecheler", "age", 29, "addr", "San Francisco", "weight", 1)

titan = graph.addVertex(T.label, "software", "name", "Titan", "lang", "java", "tag", "Graph Database", "weight", 1)

dalaro.addEdge("created", titan, "weight", 1)
mbroecheler.addEdge("created", titan, "weight", 1)
okram.addEdge("created", titan, "weight", 1)

dalaro.addEdge("knows", mbroecheler, "weight", 1)

titan.addEdge("implements", tinkerpop, "weight", 1)
titan.addEdge("supports", gremlin, "weight", 1)

// HugeGraph
javeme = graph.addVertex(T.label, "person", T.id, "javeme", "name", "Jermy Li", "age", 29, "addr", "Beijing", "weight", 1)
zhoney = graph.addVertex(T.label, "person", T.id, "zhoney", "name", "Zhoney Zhang", "age", 29, "addr", "Beijing", "weight", 1)
linary = graph.addVertex(T.label, "person", T.id, "linary", "name", "Linary Li", "age", 28, "addr", "Wuhan. Hubei", "weight", 1)

hugegraph = graph.addVertex(T.label, "software", "name", "HugeGraph", "lang", "java", "tag", "Graph Database", "weight", 1)

javeme.addEdge("created", hugegraph, "weight", 1)
zhoney.addEdge("created", hugegraph, "weight", 1)
linary.addEdge("created", hugegraph, "weight", 1)

javeme.addEdge("knows", zhoney, "weight", 1)
javeme.addEdge("knows", linary, "weight", 1)

hugegraph.addEdge("implements", tinkerpop, "weight", 1)
hugegraph.addEdge("supports", gremlin, "weight", 1)
```

### 基本概念与操作

```groovy
graph.schema().getVertexLabels() //查看所有点 label
graph.schema().getEdgeLabels() //查看所有边 label
graph.schema().getIndexLabels() //查看所以index label
graph.schema().getPropertyKeys() //查看所有属性
graph.schema.propertyKey("weight").remove()  //删除属性 weight
graph.schema.vertexLabel("person").remove() //删除点label person
graph.schema.edgeLabel("rate").remove() //删除边label rate
graph.schema.indexLabel("personByAge").remove() //删除index label personByAge

g.V()
g.V('3:Gremlin','2:TinkerPop')
g.E()
g.E('Sokram>1>>Sspmallette')
g.V().id()
g.V().label()
g.E().properties()
g.V().properties().key()
g.V().properties().value()
g.E().valueMap()
// g.V().properties().value()
g.V().values()
// g.V().properties('lang').value()
g.V().values('lang')
```

### 边的遍历操作

```groovy
// 先查询图中所有的顶点
// 然后访问顶点的OUT方向邻接点
// 注意：out()的基准必须是顶点
g.V().out()
g.V('2:TinkerPop').out()
g.V('2:TinkerPop').out('define')

g.V('2:TinkerPop').in()
g.V('2:TinkerPop').in('implements')

g.V('2:TinkerPop').both()
g.V('2:TinkerPop').both('implements', 'define')

g.V('2:TinkerPop').outE()
g.V('2:TinkerPop').outE('define')
g.V('2:TinkerPop').inE()
g.V('2:TinkerPop').inE('implements')
g.V('2:TinkerPop').bothE()
g.V('2:TinkerPop').bothE('define', 'implements')
g.V('3:TinkerPop').outE().otherV()
//等价于out()

// 通过id找到“javeme”作者顶点
// 通过out()访问其创建的软件
// 继续通过out()访问软件实现的框架
// 继续通过out()访问框架包含的软件
// 继续通过out()访问软件支持的语言
g.V('javeme').out('created').out('implements').out('contains').out('supports')
```

### has条件过滤

```groovy
// 查询label为"person"的顶点
g.V().hasLabel('person')
// 查询label为"person"或者"software"的顶点
g.V().hasLabel('person', 'software')

// 查询id为"zhoney"的顶点
g.V().hasId('zhoney')
// 查询id为“zhoney”或者“2:HugeGraph”的顶点
g.V().hasId('zhoney', '2:HugeGraph')

//has(key, value)，通过属性的名字和值来过滤顶点或边
// 查询“addr”属性值为“Beijing”的顶点
g.V().has('addr', 'Beijing')

// 查询“addr”属性值为“Beijing”的顶点
g.V().has('addr', 'Beijing')
// 查询label为“person”且“addr”属性值为“Beijing”的顶点
g.V().has('person', 'addr', 'Beijing')
// 查询“age”属性值大于20的顶点
g.V().has('age', gt(20))

g.V().properties().hasKey('lang')
g.V().properties().hasValue('Beijing')
g.V().has('age')
// 查询没有属性“age”的顶点
g.V().hasNot('age')
```

### 图查询返回结果数限制

```groovy
//count(): 统计查询结果集中元素的个数；
g.V().count()
g.V().hasLabel('person').count()

//range(m, n): 指定下界和上界的截取，左闭右开。比如range(2, 5)能获取第2个到第4个元素（0作为首个元素，上界为-1时表示剩余全部）；
g.V().hasLabel('person').range(0, -1)

//limit(n): 下界固定为0，指定上界的截取，等效于range(0, n)，语义是“获取前n个元素”。比如limit(3)能获取前3个元素；
g.V().limit(2)

//tail(n): 上界固定为-1，指定下界的截取，等效于range(count - n, -1)，语义是“获取后n个元素”。比如tail(2)能获取最后的2个元素；
g.V().tail(2)

//skip(n): 上界固定为-1，指定下界的截取，等效于range(n, -1)，语义是“跳过前n个元素，获取剩余的元素”。比如skip(6)能跳过前6个元素，获取最后2个元素。
// 跳过前5个，skip(5)等价于range(5, -1)
g.V().hasLabel('person').skip(5)
```

### 查询路径path

```groovy
/ “HugeGraph”顶点到与其有直接关联的顶点的路径（仅包含顶点）
g.V().hasLabel('software').has('name','HugeGraph').both().path()
// “HugeGraph”顶点到与其有直接关联的顶点的路径（包含顶点和边）
g.V().hasLabel('software').has('name','HugeGraph')
 .bothE().otherV().path()

// “HugeGraph”顶点到与其有直接关联的顶点的路径（包含顶点和边）
// 用“name”属性代表person和software顶点，用“weight”属性代表边
g.V().hasLabel('software').has('name','HugeGraph')
 .bothE().otherV().path().by('name').by('weight')

// “HugeGraph”顶点到与其有两层关系的顶点的所有路径（只包含顶点）
g.V().hasLabel('software').has('name','HugeGraph')
 .both().both().path()

// “HugeGraph”顶点到与其有两层关系的顶点的不含环路的路径（只包含顶点）
g.V().hasLabel('software').has('name','HugeGraph')
 .both().both().simplePath().path()

// “HugeGraph”顶点到与其有两层关系的顶点的包含环路的路径（只包含顶点）
g.V().hasLabel('software').has('name','HugeGraph')
 .both().both().cyclicPath().path()
```

### 循环操作

```groovy
// 访问某个顶点的OUT邻接点（1次）
// 注意'okram'是顶点的id
g.V('okram').repeat(out()).times(1)

// 访问某个顶点的2度双向邻接点
// 访问第1个顶点的所有邻接点（第1层）
// 再访问第1层结果顶点的邻接点（第2层）
g.V('okram').repeat(both()).times(2)
g.V('okram').repeat(out()).times(3)

// 查询顶点'okram'到顶点'Gremlin'之间的路径
// 循环的终止条件是遇到名称是'Gremlin'的顶点
g.V('okram')
 .repeat(out())
 .until(has('name', 'Gremlin'))
 .path()

// 查询顶点'okram'的所有OUT可达点的路径
g.V('okram')
 .repeat(out())
 .emit()
 .path()

// 查询顶点'okram'的所有OUT可达点的路径
// 且必须满足是'person'类型的点
g.V('okram')
 .repeat(out())
 .emit(hasLabel('person'))
 .path()

// 查询顶点'okram'到顶点'Gremlin'之间的路径
// 此外还收集过程中的'person'类型的顶点
g.V('okram')
 .repeat(out())
 .until(has('name', 'Gremlin'))
 .emit(hasLabel('person'))
 .path()
//emit()与until()搭配使用时，是“或”的关系而不是“与”的关系，满足两者间任意一个即可

// 查询顶点'okram'的3度OUT可达点路径
g.V('okram')
 .repeat(out())
 .until(loops().is(3))
 .path()

// 查询顶点'okram'到顶点'Gremlin'之间的路径
// 且之间只相差2跳的距离
// 其中的and()是指两个条件都满足
g.V('okram')
 .repeat(out())
 .until(has('name', 'Gremlin')
        .and().loops().is(2))
 .path()

// 已知两个顶点'okram'和'javeme'，
// 通过任意关系来找到这两点之间的路径
// 且限制了最大深度为3
// 若存在那么第一条结果即是最短路径
g.V('okram')
 .repeat(bothE().otherV().simplePath())
 .until(hasId('javeme').and().loops().is(lte(3)))
 .hasId('javeme')
 .path()
```

### 查询结果排序

```groovy
//order().by(incr): 将结果以升序输出，这也是默认的排序方式；
// 以默认排序输出所有顶点的"name"属性值
g.V().values('name').order()
g.V().values('name').order().by(incr)

//order().by(decr): 将结果以降序输出；
g.V().values('name').order().by(decr)

//order().by(shuffle): 将结果以随机序输出，每次执行结果顺序都可能不一样。
g.V().values('name').order().by(shuffle)

//order().by(key): 将结果按照元素属性key的值升序排列，与order().by(key, incr)等效；
g.V().hasLabel('person').order().by('age')
//order().by(key, incr): 将结果按照元素属性key的值升序排列；
g.V().hasLabel('person').order().by('age').values('age')
```

### 数据分组与去重

```groovy
// 不指定任何维度进行分组
g.V().hasLabel('person').group()

// 不指定任何维度进行分组
// 但数据集中有重复的元素
// 重复的元素将会被分为一组
g.V().both().hasLabel('person').group()

// 根据年龄进行分组
g.V().hasLabel('person').group().by('age')

// 根据年龄进行分组
// 并统计各个年龄的人数
g.V().hasLabel('person')
 .group().by('age').by(count())

// 根据顶点类别进行分组
// 并统计各个类别的数量
g.V().group().by(label).by(count())

// 不指定任何维度进行分组计数
g.V().hasLabel('person').groupCount()

// 进行分组计数
g.V().hasLabel('person').groupCount()
g.V().both().hasLabel('person').groupCount()
g.V().hasLabel('person').groupCount().by('age')

// 对一组含有重复顶点的数据进行去重
g.V().both().hasLabel('person').dedup()
g.V().hasLabel('person').values('age').dedup()

// 从各个年龄的人中选出一个代表
g.V().hasLabel('person').dedup().by('age')

// 根据地域分组，并得到各个组的平均年龄
g.V().hasLabel('person').group()
 .by('addr').by(values('age').mean())
// 统计顶点的边数量的分布情况
g.V().groupCount().by(bothE().count())
```

### 条件和过滤

| Predicate         | Description                                      |
| :---------------- | ------------------------------------------------ |
| eq(object)        | 传入的对象等于目标object?                        |
| neq(object)       | 传入的对象不等于目标object?                      |
| lt(number)        | 传入的数字小于目标number?                        |
| lte(number)       | 传入的数字小于或等于目标number?                  |
| gt(number)        | 传入的数字大于目标number?                        |
| gte(number)       | 传入的数字大于或等于目标number?                  |
| inside(low,high)  | 传入的数字大于low且小于high?                     |
| outside(low,high) | 传入的数字小于low或者大于high?                   |
| between(low,high) | 传入的数字大于等于low且小于high?                 |
| within(objects…)  | 传入的对象等于目标对象列表objects中的任意一个?   |
| without(objects…) | 传入的对象不等于目标对象列表objects中的任何一个? |

```groovy
// (3 == 2)
eq(2).test(3)
// ('d' == 'a' || 'd' == 'b' || 'd' == 'c')
within('a','b','c').test('d')
// (3 > 1 && 3 < 4)
inside(1,4).test(3)

// and()连接的predicate，是一个新的predicate
within(1,2,3).and(not(eq(2))).test(3)
// or()连接的predicate，是一个新的predicate
inside(1,4).or(eq(5)).test(3)

// 查看“zhoney”的合作伙伴
// where(P)方式
g.V('zhoney').as('a')
 .out('created').in('created')
 .where(neq('a'))

// 查看“zhoney”的合作伙伴
// where(String, P)方式
g.V('zhoney').as('a')
 .out('created').in('created').as('b')
 .where('a',neq('b'))

// “spmallette”开发过不止一个软件的合作伙伴
// where(Traversal)方式
g.V('spmallette').out('created').in('created')
 .where(out('created').count().is(gt(1)))
 .values('name')

// 查询”被别人认识“
// 且认识自己的人的年龄大于自己的年龄的人
g.V().as('a')
 .out('knows').as('b')
 .where('a', gt('b')).by('age')

//  where()与as()+select()配合使用
// 查看“zhoney”的合作伙伴，并将“zhoney”及其合作伙伴的名字以map输出
// select().where()方式
g.V('zhoney').as('a')
 .out('created').in('created').as('b')
 .select('a','b').by('name')
 .where('a',neq('b'))

//  where()与match()配合使用
// 查看“zhoney”的合作伙伴，并将“zhoney”及其合作伙伴的名字以map输出
// match().where()方式
g.V('zhoney').match(__.as('a').out('created').as('b'),
                    __.as('b').in('created').as('c')).
                    where('a', neq('c'))
             .select('a','c').by('name')


// 查找图中的“person”顶点
// lambda方式
g.V().filter {it.get().label() == 'person'}
```

### 逻辑运算

```groovy
// 筛选出顶点属性“age”等于28的属性值，与`is(P.eq(28))`等效
g.V().values('age').is(28)
// 筛选出顶点属性“age”大于等于28的属性值
g.V().values('age').is(gte(28))
// 筛选出顶点属性“age”属于区间（27，29）的属性值
g.V().values('age').is(inside(27, 29))

// 所有包含出边“supports”的顶点的名字“name”
g.V().and(outE('supports')).values('name')
// 所有包含出边“supports”和“implements”的顶点的名字“name”
g.V().and(outE('supports'), outE('implements')).values('name')

// 包含边“created”并且属性“age”为28的顶点的名字“name”
g.V().and(outE('created'), values('age').is(28)).values('name')
// 包含边“created”并且属性“age”为28的顶点的名字“name”
g.V().where(outE('created')
            .and()
            .values('age').is(28))
 .values('name')

// 所有包含出边“supports”或“implements”的顶点的名字“name”
g.V().or(outE('supports'),outE('implements')).values('name')

// 筛选出所有不是“person”的顶点的“label”
g.V().not(hasLabel('person')).label()
// 筛选出所有包含不少于两条（大于等于两条）“created”边的“person”的名字“name”
g.V().hasLabel('person').not(out('created').count().is(lt(2))).values('name')
```

### 统计运算

```groovy
// 计算所有“person”的“age”的总和
g.V().hasLabel('person').values('age').sum()
// 计算所有“person”的“created”出边数的总和
g.V().hasLabel('person').map(outE('created').count()).sum()

// 计算所有“person”的“age”中的最大值
g.V().hasLabel('person').values('age').max()

// 计算所有“person”的“age”中的最小值
g.V().hasLabel('person').values('age').min()

// 计算所有“person”的“age”的均值
g.V().hasLabel('person').values('age').mean()
```

### 数学运算

- math() 支持by()，其中多个by() 按照在math()运算表达式中首次引用变量的顺序应用。
- 保留变量_是指传入math()的当前遍历器对象。
  math()支持的运算符包括：+，-，*，/，%，^

math()支持的内嵌函数包括：

- `abs`: absolute value，绝对值

- `ceil`: nearest upper integer，向上最接近的整数
- `floor`: nearest lower integer，向下最近接的整数

- `sqrt`: square root，平方根
- `cbrt`: cubic root，立方根

### 路径选取与过滤

```groovy
//Step as()...select()：对路径中结果进行选取
// 从路径中选取第1步和第3步的结果作为最终结果
g.V('2:HugeGraph').as('a')
 .out().as('b')
 .out().as('c')
 .select('a', 'c')

// 从集合中选择最后一个元素
g.V('2:HugeGraph').as("a")
 .repeat(out().as("a")).times(2)
 .select(last, "a")

// 通过by()来指定选取的维度
g.V('2:HugeGraph').as('a')
 .out().as('b')
 .out().as('c')
 .select('a', 'c')
 .by('name').by('name')

// 从map中选择指定key的值
g.V().valueMap().select('tag').dedup()

//Step as()...where()：以条件匹配的方式进行路径结果选取
// 选取满足第1步和第3步“lang”属性相等的路径
g.V('2:HugeGraph').as('a')
 .out().as('b').out().as('c')
 .where('a', eq('c')).by('lang')
 .select('a', 'b', 'c').by(id)

//Step as()+match()：以模式匹配的方式进行路径结果选取
// 选取满足两个模式的路径：
// 1.第3步有OUT节点
// 2.第3步的OUT节点的指定路径不允许回到第二步的节点
g.V('2:HugeGraph').as('a').out().as('b')
 .match(__.as('b').out().as('c'),
        __.not(__.as('c').out().in('define').as('b')))
 .select('a','b','c').by(id)

//Step as()+debup()：路径去重
g.V('2:HugeGraph').as('a')
 .out().as('b').out().as('c').in()
 .dedup('a', 'b', 'c').path()
```

### 分支

```groovy
// 查找所有的“person”类型的顶点
// 如果“age”属性小于等于20，输出他的朋友的名字
// 如果“age”属性大于20，输出他开发的软件的名字
// choose(condition, true-action, false-action)
g.V().hasLabel('person')
 .choose(values('age').is(lte(20)),
         __.in('knows'),
         __.out('created')).values('name')

// 查找所有的“person”类型的顶点
// 如果“age”属性等于0，输出名字
// 如果“age”属性等于28，输出年龄
// 如果“age”属性等于29，输出他开发的软件的名字
// choose(predicate).option().option()...
g.V().hasLabel('person')
 .choose(values('age'))
 .option(0, values('name'))
 .option(28, values('age'))
 .option(29, __.out('created').values('name'))

//如果choose(predicate, true-traversal, false-traversal)中false-traversal为空或者是identity()，则不满足条件的对象直接通过choose()
// 查找所有顶点，
// 类型为“person”的顶点输出其创建的软件的“name”属性
// 否则输出顶点自身的“name”属性
g.V().choose(hasLabel('person'),  
             out('created'))
 .values('name')
//等同于
g.V().choose(hasLabel('person'), 
             out('created'), 
             identity())
 .values('name')

// 查找所有类型为“person”的顶点，
// “name”属性为“Zhoney Zhang”的输出其“age”属性
// 否则输出顶点的“name”属性
g.V().hasLabel('person')
 .choose(values('name'))
 .option('Zhoney Zhang', values('age'))
 .option(none, values('name'))
```

### 合并

```groovy
// Step coalesce()
// 按优先级寻找到顶点“HugeGraph”的以下边和邻接点，找到一个就停止
// 1、“implements”出边和邻接点
// 2、“supports”出边和邻接点
// 3、“created”入边和邻接点
g.V('2:HugeGraph').coalesce(outE('implements'), outE('supports'), inE('created')).inV().path().by('name').by(label) 

// Step optional()
// 查找顶点"linary"的“knows”出顶点，如果没有就返回"linary"自己
g.V('linary').optional(out('knows'))

// Step union()
// 寻找顶点“linary”的出“created”顶点，邻接“knows”顶点，并将结果合并
g.V('linary').union(out('created'), both('knows')).path()
// 寻找顶点“HugeGraph”的入“created”顶点（创作者），出“implements”和出“supports”顶点，并将结果合并
g.V('2:HugeGraph').union(__.in('created'), out('implements'), out('supports'), out('contains')).path()

```

### 结果聚集与展开

```groovy
// 收集第1步的结果到集合'x'中
// 注意：不影响后续结果
g.V('2:HugeGraph').out().aggregate('x')

// 通过by()来指定聚集的维度
g.V('2:HugeGraph').out()
 .aggregate('x').by('name')
 .cap('x')

// 以Lazy方式收集，后续步骤使用limit限制时，
// 路径中取到第2个结果时将会停止，
// 因此集合中有2个元素。
g.V().store('x').by('name').limit(1).cap('x')

// 将集合‘x’展开(层级变少了)
g.V('2:HugeGraph').out()
 .aggregate('x').by('name')
 .cap('x').unfold()

// 将属性折叠起来(层级变深)
g.V('2:HugeGraph').out()
 .values('name').fold()

// 统计所有'name'属性的长度
// 其中通过lambuda表达式累加字符串长度
g.V('2:HugeGraph').out().values('name')
 .fold(0) {a,b -> a + b.length()}
```

