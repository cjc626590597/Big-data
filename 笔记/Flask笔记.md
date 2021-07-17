## Flask笔记

##### 1. 自动生成目录
在命令敲 `pip freeze > requirements.txt`  即可生成依赖目录，其他项目想快速下载，则将 `requirements.txt` 复制到其项目文件夹里，然后`pip install -r requirements.txt` 即可开始下载

##### 2. 简单Flask模板
   ```python
   #引入Flask扩展
   from flask import Flask
   
   #需要传入__name__,作用是为了确定资源所在资源的路径
   app = Flask(__name__)
   
   #路由解析，通过用户访问的路径，匹配相应的函数
   @app.route('/')
   def hello_world():
       return '欢迎光临!'
   
   #启动程序
   if __name__ == '__main__':
       app.run(debug=True)
   ```

##### 3. 请求 路由默认只支持GET，如果需要增加，需要自行制定
   ```python 
   @app.route('/result',methods=['POST','GET'])
   ```

##### 4. 使用同一个视图函数 来显示不同的信息

   ```python
   #<>定义路由的参数，<>内需要起个名字
   @app.route("/user/<name>")
   def welcome(name):
       # 需要在视图函数里的()内填入参数名，那么后面的代码才能去使用
       
       #需要对路由进行访问优化，默认为字符串，但是可以在名字前加类型来限定例如@app.route("/user/int:<id>"),允许int或float
       
       return "你好,%s"%name
   ```

##### 5. Jinja2模板 返回网页模板并填充数据

   ```python
   #页面返回一个网页模板
   from flask import render_template
   #并给模板填充数据
   @app.route("/test/jinja2")
   def jinja2():
       #填充date
       date = "星期五"
       return render_template("test/jinja2.html",date_html = date)
   ```

   ```html
   <body>
       今天是{{ date_html }}.<br/>
   </body>
   ```

##### 6. Jinja2 传入其他类型的值

   ```python
   @app.route("/")
   def index2():
       time = datetime.date.today()     #普通变量
       name = ["小张","小王","小赵"]     #列表变量
       task = {"任务":"打扫卫生","时间":"3小时"}     #字典变量
       return render_template("index.html",var = time, list = name, task=task)
   ```

   ```html
   <body>
         今天是{{ var }},欢迎光临。<br/>
         我是: {{ list[1] }}<br/>
         我的任务是: {{ task["时间"] }}<br/>
         今天值班的有：<br/>
         {% for data in list %}    <!--用大括号和百分号括起来的是控制结构，还有if-->
             <li>{{ data }}</li>
         {% endfor %} <!--快捷键 先写for或if然后按Tab补全-->
         {% if var != null %}
             {{ var }}<br/>
         {% endif %}
       	 {% if 条件1 %}
 		  语句块1
           {% elif 条件2 %}
           语句块2
           {% else %}
           不符合所有条件
           {% endif %}
   </body>
   ```

   显示效果：    <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210126122228176.png" alt="image-20210126122228176" style="zoom:80%;" />

##### 6. jinja2 打印表格

   ```html
     任务:<br/>         <!--了解如何在页面打印表格，以及如何迭代-->
         <table border="1">
             {% for key,value in task.items() %}    <!--[(key,value),(key,value),(key,value)]-->
             <tr>
               <td>{{key}}</td>
               <td>{{value}}</td>
             </tr>
             {% endfor %}
         </table>
   ```
   显示效果：  <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210126122248174.png" alt="image-20210126122248174" style="zoom:80%;" />

##### 7. 转换器

   ```html
         {{ str | upper }} <br/>   <!--字符串大写-->
         {{ str | reverse }} <br/> <!--字符串反转-->
         {{ str | upper | lower }} <br/>   <!--字符串大写再小写-->
   ```

##### 8. WTF表单 实现一个简单的登录的逻辑处理

   ```python
   from flask import request
   @app.route('/test/login',methods=['POST','GET'])
   def login():
       # 1.判断请求方式
       if request.method == 'POST':
           # 2.获取请求的参数
           username = request.form.get('username')
           password = request.form.get('password')
           password2 = request.form.get('password2')
           # 3.判断参数是否填写 & 密码是否相同
           if not all([username, password, password2]):
               print('参数不完整')
           elif password != password2:
               print('密码不一致')
           else:
               # 4.如果判断都没有问题，返回success
               return 'success'
       return render_template("/test/login.html")
   ```

   ```html
   <form method="post">
       <p>账号:<input type="text" name="username"></p>
       <p>密码:<input type="text" name="password"></p>
       <p>确认密码:<input type="text" name="password2"></p>
       <p><input type="submit" value="提交"></p>
   </form>
   ```

##### 9. flash消息闪现 给模板传递消息

   ```python
   from flask import flash
   #flash-->需要对内容加密，因此要设置secret_key，做加密消息的混淆
   app.secret_key = 'cccc'
   
   if not all([username, password, password2]):
       flash(u'参数不完整')
   elif password != password2:
       flash(u'密码不一致')
   ```

   ```html
   {# 获取遍历闪现的消息 #}
   {% for message in get_flashed_messages() %}
       {{ message }}
   {% endfor %}
   ```

##### 10. 使用WTF实现表单

```python
   from flask_wtf import FlaskForm
   from wtforms import StringField,PasswordField,SubmitField
    #自定义表单
    class LoginForm(FlaskForm):
        username = StringField('用户名:')
        password = PasswordField('密码:')
        password2 = PasswordField('确认密码:')
        submit = SubmitField('提交')

    @app.route('/test/form',methods=['GET','POST'])
    def login2():
        login_form = LoginForm()
        return render_template('test/form.html',form=login_form)
```

```html
    <form method="post">
        {{ form.username.label }}{{ form.username }}<br>
        {{ form.password.label }}{{ form.password }}<br>
        {{ form.password2.label }}{{ form.password2 }}<br>
        {{ form.submit }}<br>
    </form>
```

##### 11. 使用WTF验证函数


   ```python
   from flask import render_template
    #自定义表单
    class LoginForm(FlaskForm):
        username = StringField('用户名:', validators=[DataRequired()])
        password = PasswordField('密码:', validators=[DataRequired()])
        password2 = PasswordField('确认密码:', validators=[DataRequired(), 
                                                       EqualTo('password','密码填入的不一致')])
        submit = SubmitField('提交')

    @app.route('/test/form',methods=['GET','POST'])
    def login2():
        login_form = LoginForm()
        # 1.判断请求方式
        if request.method == 'POST':
            # 2.获取请求的参数
            username = request.form.get('username')
            password = request.form.get('password')
            password2 = request.form.get('password2')
            # 3.验证参数，WTF可以一句话就实现所有的验证
            if login_form.validate_on_submit():
                print(username,password)
                return 'success'
            else:
                flash('参数有误')
        return render_template('test/form.html',form=login_form)
   ```

   ```html
	<form method="post">
        {# 需要CSRF token #}
        {{ form.csrf_token() }}
        {{ form.username.label }}{{ form.username }}<br>
        {{ form.password.label }}{{ form.password }}<br>
        {{ form.password2.label }}{{ form.password2 }}<br>
        {{ form.submit }}<br>
        {# 获取遍历闪现的消息 #}
        {% for message in get_flashed_messages() %}
            {{ message }}
        {% endfor %}
    </form>
   ```

##### 12. SQLAlchemy配置


   ```python
    from flask_sqlalchemy import SQLAlchemy
    #配置数据库地址
    app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://root:a123456@127.0.0.1/flask_sql_demo'  #需修改
    #跟着数据库的修改 --> 不建议开启 未来的版本会移除
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    db = SQLAlchemy(app)
   ```

##### 13. 定义数据类型

```python
#数据库的模型，需要基层db.Model
class Role(db.Model):
    #定义表名
    __tablename__ = 'roles'   

    #定义字段
    # db.column表示一个字段
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(16), unique=True)

class User(db.Model):
    __tablename__ = 'users'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(16), unique=True)
    #db.ForeignKey是外键，表明.id形式
    role_id = db.Column(db.Integer, db.ForeignKey('roles.id'))
```

##### 14. 创建表格

```python
#由于版本变动
import pymysql
#配置数据库地址
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://root:a123456@127.0.0.1:3306/flask_sql_demo'
#删除表
db.drop_all()
#创建表
db.create_all()

#或者
#终端 安装 pip install mysql-connector
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+mysqlconnector://root:a123456@127.0.0.1:3306/flask_books'
```

##### 15. 增删改

```python
#增
role = Role(name='admin')
db.session.add(role)
db.session.commit()
user = User(name='itheima', role_id=role.id)
db.session.add(user)
db.session.commit()
#改
User.query.filter(User.id==4).update({'id': '3'})
db.session.commit()
#删
db.session.delete(user)
db.session.commit()
```

##### 16. 关系引用

```python
class Role(db.Model):
    #在一的一方，写关联
    #users = db.relationship('User')：表示和User模型发生了关联，增加一个users属性
    #backref = 'role'：表示role是User要用的属性
    users = db.relationship('User',backref = 'role')

    #repr()方法显示一个可读字符串
    def __repr__(self):
        return '<Role: %s %s>'%(self.name,self.id)

class User(db.Model):
    # User希望有role属性，但这个属性的定义，需要在另外一个模型中定义

    # repr()方法显示一个可读字符串
    def __repr__(self):
        return '<Role: %s %s %s %s>' % (self.name, self.id, self.email, self.password)
```

```python
#实现关系引用查询
#例子
role.users
user1.role
user2.role.name
```

##### 17. 查询

```python
#查询所有用户
User.query.all()
#查询有多少个用户
User.query.count()
#查询第一个用户
User.query.first()
#查询id为4的用户
User.query.get(4)
User.query.filter_by(id=4).first()
User.query.filter(User.id==4).first()
```

##### 18. 项目实战

```python
'''
1.配置数据库
    a.导入SQLAYchemy扩展
    b.创建db对象，并配置参数
    c.终端创建数据库
2.添加书和作者模型
    a.模型继承db.Model
    b.__tablename__:表名
    c.db.Column:字段
    d.db.relationship:关系引用
3.添加数据
4.使用模板显示数据库数据
    a.查询所有的作业信息，让信息传递给模板
    b.模板中按照格式，依次for循环作者和书籍即可（作者获取书籍，用的关系引用）
5.使用WTF显示表单
    a.自定义表单类
    b.模板中显示
    c.secret_key/编码/csrf_token
6.实现相关的CRUD操作
    a.增加书籍
    b.删除书籍 url_for的使用 / for else的使用 / redirect的使用
    c.删除作者
'''

# 删除书籍 --> 网页中删除 --> 点击需要发送书籍的ID给指定的路由 --> 路由需要接受参数
@app.route('/delete_book/<book_id>')
def delete_book(book_id):

    # 1. 查询数据库，是否有该ID的书，如果有就删除，没有就提示错误
    book = Book.query.get(book_id)

    # 2. 如果有就删除
    if book:
        try:
            db.session.delete(book)
            db.session.commit()
        except Exception as e:
            print(e)
            flash('删除书籍出错')
            db.session.rollback()
    else:
        flash('书籍找不到')

    # 如何返回当前网址-->重定向
    # return redirect('www.itheima.com')
    # return redirect('/')

    # redirect: 重定向，需要传入网络/路由地址
    # url_for('books'): 需要传入视图函数名，返回改视图函数对应的路由地址
    return redirect(url_for('books'))

# 删除作者 --> 网页中删除 --> 点击需要发送书籍的ID给指定的路由 --> 路由需要接受参数
@app.route('/delete_author/<author_id>')
def delete_author(author_id):

    # 1. 查询数据库，是否有该ID的书，如果有先删除（先删书，再删作者），没有就提示错误
    author = Author.query.get(author_id)

    # 2. 如果有先删除（先删书，再删作者）
    if author:
        try:
            # 查询之后直接删除
            Book.query.filter_by(author_id=author.id).delete()

            # 删除作者
            db.session.delete(author)
            db.session.commit()
        except Exception as e:
            print(e)
            flash('删除作者出错')
            db.session.rollback()
    else:
        flash('作者找不到')

    return redirect(url_for('books'))
```

```html
{% for author in authors %}
   <li>{{ author.name }}<a href="{{ url_for("delete_author", author_id=author.id) }}">删除</a></li>
    <ul>
        {% for book in author.books %}
            <li>{{ book.name }}<a href="{{ url_for("delete_book", book_id=book.id) }}">删除</a></li>
        {% else %}
            <li>无</li>
        {% endfor %}
    </ul>
{% endfor %}
```