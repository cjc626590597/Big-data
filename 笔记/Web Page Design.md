# Web Page Design

### HTML

##### 1. Layout

```html
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Title</title>
</head>
<body>
<div style="margin-top: 500px"></div>
</body>
</html>
```

##### 2. Heading

```html
    <!-- -->
    <h1>this is heading one</h1>
    <h2>this is heading two</h2>
    <h6>this is heading six</h6>
```

##### 3. Paragraph

```html
    <p>
        this is a paragraph<br>
        this is a paragraph
        this is a paragraph
        this is a paragraph
        this is a paragraph
    </p>
```

##### 4. Style

```html
    <p>
        <strong>this is a paragraph</strong><br>
        <em>this is a paragraph</em><br>
        <a href="http://google.com" target="_blank"> this is a paragraph</a><br>
    </p>
```

##### 5. List

```html
    <!-- Lists -->
    <ul>
        <li>List Item 1</li>
        <li>List Item 2</li>
        <li>List Item 3</li>
        <li>List Item 4</li>
    </ul>

    <ol>
        <li>List Item 1</li>
        <li>List Item 2</li>
        <li>List Item 3</li>
        <li>List Item 4</li>
    </ol>
```

##### 6. Table

```html
    <!-- Table -->
    <table>
        <thead>
            <tr>
                <th>Name</th>
                <th>Email</th>
                <th>Age</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>Tim</td>
                <td>123@123.com</td>
                <td>18</td>
            </tr>
            <tr>
                <td>Tom</td>
                <td>1234@1234.com</td>
                <td>18</td>
            </tr>
            <tr>
                <td>Jane</td>
                <td>1235@1235.com</td>
                <td>18</td>
            </tr>
        </tbody>
    </table>

    <br>
    <hr>
    <br>
```

##### 7. Form

```html
    <!-- Forms -->
    <form action="process.php" method="POST">
        <div>
            <label>First name</label>
            <input type="text" name="firstName"
            placeholder="enter first name">
        </div>
        <br>
        <div>
            <label>Last name</label>
            <input type="text" name="lastName">
        </div>
        <br>
        <div>
            <label>Email</label>
            <input type="email" name="email">
        </div>
        <br>
        <div>
            <label>Email</label>
            <textarea name="message" id="" cols="30" rows="5"></textarea>
        </div>
        <br>
        <div>
            <label>Gender</label>
            <select name="gender" id="">
                <option value="male">Male</option>
                <option value="female">Female</option>
                <option value="other">Other</option>
            </select>
        </div>
        <br>
        <div>
            <label>Age</label>
            <input type="number" name="age" value="30">
        </div>
        <br>
        <div>
            <label>Birthday</label>
            <input type="date" name="birthday">
        </div>
        <br>
        <input type="submit" name="submit" value="Submit">
    </form>
    <br>
```

##### 8. Button

```html
    <!-- Button -->
    <button>Click Me</button>
    <br>
```

##### 9. Image

```html
    <!-- Img -->
    <a href="../../static/img/Snipaste_2021-02-03_16-40-55.png">
        <img src="../../static/img/Snipaste_2021-02-03_16-40-55.png"
        alt="My sample Image" width="200">
    </a>
    <br>
```

##### 10. Quotations

```html
    <!--  Quotations -->
    <blockquote cite="http://traversymedia.com">
        this is a paragraph<br>
        this is a paragraph
        this is a paragraph
        this is a paragraph
        this is a paragraph
    </blockquote>
```

##### 11. Abbreviation

```html
    <!-- abbreviation -->
    <p>The <abbr title="World Wide Web">WWW</abbr> is awesome</p>

    <p><cite>HTML crash course by university</cite></p>
```

##### 12. Display index.html

```html
<div style="margin-top: 500px"></div>
```

![image-20210203203705634](C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210203203705634.png)

##### 13. Meta data & CSS style

```html
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>My Blog</title>
    <meta name="description" content="Awesome blog by Traversy Media">
    <meta name="keywords" content="web design blog, web dev blog, traversy media">
    <style type="text/css">
        #main-header{
            text-align: center;
            background-color: black;
            color: white;
            padding: 10px;
        }

        #main-footer{
            text-align: center;
            font-size: 18px;
        }
    </style>
</head>
```

##### 14. Header &  Link

```html
<body>
    <header id="main-header">
        <h1>My website</h1>
    </header>

    <a href="index.html">Go to the index</a>
```

##### 15. Section & Article

```html
    <section>
        <article class="post">
            <h3>Blog Post One</h3>
            <small>Posted by Brad on July 17</small>
            <p>
                this is a paragraph<br>
                this is a paragraph
                this is a paragraph
                this is a paragraph
                this is a paragraph
            </p>
            <a href="post.html">read more</a>
        </article>

        <article class="post">
            <h3>Blog Post Two</h3>
            <small>Posted by Brad on July 17</small>
            <p>
                this is a paragraph<br>
                this is a paragraph
                this is a paragraph
                this is a paragraph
                this is a paragraph
            </p>
            <a href="post.html">read more</a>
        </article>

        <article class="post">
            <h3>Blog Post Three</h3>
            <small>Posted by Brad on July 17</small>
            <p>
                this is a paragraph<br>
                this is a paragraph
                this is a paragraph
                this is a paragraph
                this is a paragraph
            </p>
            <a href="post.html">read more</a>
        </article>

        <article class="post">
            <h3>Blog Post Four</h3>
            <small>Posted by Brad on July 17</small>
            <p>
                this is a paragraph<br>
                this is a paragraph
                this is a paragraph
                this is a paragraph
                this is a paragraph
            </p>
            <a href="post.html">read more</a>
        </article>
    </section>
```

##### 16. Aside

```html
    <aside>
        <h3>Categories</h3>
        <nav>
        <ul>
            <li><a href="#">Category 1</a></li>
            <li><a href="#">Category 2</a></li>
            <li><a href="#">Category 3</a></li>
        </ul>
        </nav>
    </aside>
```

##### 17. Footer

```html
    <footer id="main-footer">
        <p>Copyright &copy; 2017, My website</p>
    </footer>
```

##### 18. Display blog.html

![image-20210203203449162](C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210203203449162.png)



### CSS 

浏览器页面css样式更新ctrl+shift+R

##### 1. Style

- Inline CSS: Directly in the html

```HTML
<head>
    <meta charset="UTF-8">
    <title>CSS</title>
    <style type="text/css">
        h1{
            color: blue;
        }
	</style>
</head>
```

- Internal CSS：Using <style> tags within a single document

```html
<h1 style="color: red">Hello World</h1>
```

- External Css：Linking an external.css file

```html
<head>
    <meta charset="UTF-8">
    <title>CSS</title>
    <link rel="stylesheet" type="text/css"
    href="../../static/css/style.css">
</head>
```

```css
h1{
    color:green;
}
```

##### 2. Body

```css
body{
    background-color:#f4f4f4;
    color:#555555;   /* color of text */

    font-family:Arial, Helvetica, scans-serif;
    font-size: 16px;
    font-weight: normal;
    /*Same as above*/
    font: normal 16px Arial,Helvetica, sans-serif ;

    line-height:1.6em;
}
```

 <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210205171640872.png" alt="image-20210205171640872" style="zoom:50%;" />

##### 3. Box-model

 <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210205175409068.png" alt="image-20210205175409068" style="zoom:50%;" />

```css
.container{
    width: 80%;
    margin: auto;
}

.box-1{
    background-color:#333333;
    color: #f4f4f4;

    border-top: 5px red solid;
    border-right: 5px red solid;
    border-bottom: 5px red solid;
    border-left: 5px red solid;
    /* same as above */
    border: 5px red solid;
    border-width: 3px;
    border-bottom-width: 10px;
    border-top-style: dotted;

    padding-top: 20px;
    padding-right: 20px;
    padding-bottom: 20px;
    padding-left: 20px;
    /* same as above */
    padding: 20px;

    margin-top:20px;
    margin:20px 0;
}
```

 <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210205180402203.png" alt="image-20210205180402203" style="zoom:50%;" />

##### 4. Tag

```css
.box-1 h1{
    font-family: Tahoma;
    font-weight: 800;
    font-style: italic;
    text-decoration: underline;
    text-transform: uppercase;
    letter-spacing: 0.2em;
    word-spacing: 1em;
}
```

 <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210205184612265.png" alt="image-20210205184612265" style="zoom:50%;" />

##### 5. Categories

```css
a{
    text-decoration: none;
    color:#000000;
}

a:hover{
    color: red;
}

a:active{
    color: green;
}

a:visited{
    color: yellow;
}

.categories{
    border: 1px #ccc solid;
    padding: 10px;
    border-radius: 15px;
}

.categories h2{
    text-align: center;
}

.categories ul{
    padding: 0;
    padding-left: 20px;
    list-style: square;
    list-style: none;
}

.categories li{
    padding-bottom: 6px;
    border-bottom: dotted 1px #333;
    list-style-image: url("../img/check.png");
}
```

 <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210205184720098.png" alt="image-20210205184720098" style="zoom:50%;" />

##### 6. Form

```css
.button{
    background-color: #333;
    color: #fff;
    padding: 10px 15px;
    border: none;
}

.button:hover{
    background: red;
    color: #fff;
}

.my-form{
    padding: 20px;
}

.form-group{
    padding-bottom: 15px;
}

.my-form label{
    display: block;
}

.my-form input[type="text"], .my-form textarea{
    padding: 8px;
    width: 100%;
}
```

 <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210205184847324.png" alt="image-20210205184847324" style="zoom:50%;" />

##### 7. Block

```css
.block{
    font-size: 5px;
    float: left;
    width: 33.3%;
    border: 1px solid #ccc;
    padding: 10px;
    box-sizing: border-box;
}

#main-block{
    font-size: 5px;
    float: left;
    width: 70%;
}

#sidebar{
    font-size: 5px;
    float: right;
    width: 30%;
    background-color: #333;
    color: #fff;
    padding: 15px;
    box-sizing: border-box;
}
```

 <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210207115739921.png" alt="image-20210207115739921" style="zoom:50%;" />

##### 8. Position: relative/absolute

```css
.p-box{
    width: 500px;
    height: 300px;
    border: 1px solid #000;
    margin-top: 30px;
    position: relative;
    background-image: url("../../static/img/light.png");

    background-repeat: no-repeat;
    /*background-position: 100px 200px;*/
    background-position: center top;
}

.p-box h1{
    position: absolute;
    top: 100px;
    left: 200px;
}

.p-box h2{
    position: absolute;
    bottom: 40px;
    right: 200px;
}
```

 <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210207120123232.png" alt="image-20210207120123232" style="zoom:50%;" />

##### 9. List、fixed

```css
.my-list li:first-child{
    background: red;
}

.my-list li:last-child{
    background: blue;
}

.my-list li:nth-child(5){
    background: yellow;
}

.my-list li:nth-child(even){
    background: grey;
}

.fix-me{
    position: fixed;
    top:400px;
}
```

 <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210207115923092.png" alt="image-20210207115923092" style="zoom:50%;" />

 <img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210207120501408.png" alt="image-20210207120501408" style="zoom:30%;" />

##### 10. Website

```css
body{
    background-color: #f4f4f4;
    color: #555555;
    font-family: Arial, Helvetica, sans-serif;
    font-size: 16px;
    line-height: 1.6em;
    margin: 0;
}

.container{
    width: 80%;
    margin: auto;
    overflow: hidden;
}

#main-header{
    background-color: coral;
    color: #ffffff;
}

#navbar{
    background-color: #333333;
    color: #ffffff;
}

#navbar ul{
    padding: 0;
    list-style: none;
}

#navbar li{
    display: inline;
}

#navbar a{
    color: #ffffff;
    text-decoration: none;
    font-size: 18px;
    padding-right: 15px;
}

#showcase{
    background-image: url("../../static/img/showcase.png") ;
    background-position: center right;
    height: 300px;
    margin-bottom: 30px;
    text-align: center;
}

#showcase h1{
    color: #ffffff;
    font-size: 50px;
    line-height: 1.6em;
    padding-top: 30px;
}

#main{
    float: left;
    width: 70%;
    padding: 0 30px;
    box-sizing: border-box;
}

#sidebar{
    float: left;
    width: 30%;
    background: #333333;
    color: #ffffff;
    padding: 10px;
    box-sizing: border-box;
}

#main-footer{
    background: #333333;
    color: #ffffff;
    text-align: center;
    padding: 20px;
    margin-top: 40px;
}

@media(max-width: 600px){
    #main{
        width: 100%;
        float: none;
    }
    #sidebar{
        width: 100%;
        float: none;
    }
}
```

<img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210207120247916.png" alt="image-20210207120247916" style="zoom:50%;" />



### XML

```xml
<bookstore> 
    <book category="children">  
        <title>Harry Potter</title>  
        <author>J K. Rowling</author>  
        <year>2005</year>   
        <price>29.99</price>
    </book> 
    <book category="web">  
        <title>Learning XML</title>  
        <author>Erik T. Ray</author> 
        <year>2003</year>  
        <price>39.95</price>
    </book>
</bookstore>
```

In the example above:
-<title>, <author>, <year>, and <price> have **text content** because they contain text (like 29.99).
<bookstore> and <book> have **element contents**, because they contain elements.
<book> has an **attribute** (category="children").  XML Attributes for Metadata.

The bookstore is the **root element** of the document.
The next line starts a <book> element.
The <book> elements have **4 child elements**: <title>, <author>, <year>, <price>.

Source: https://www.w3schools.com/xml/xml_http.asp



### JSON

In JSON, values must be one of the following data types: string, number, object (JSON object), array, boolean, null

```json
{
 "name":"John",
 "age":30,
 "cars": {
          "car1":"Ford",
          "car2":"BMW",
          "car3":"Fiat"
         },
 "owners":[ "Tim", "Tom", "John" ]
 }
```

Source: https://www.w3schools.com/js/js_json_intro.asp







<img src="C:\Users\62659\AppData\Roaming\Typora\typora-user-images\image-20210220182408616.png" alt="image-20210220182408616" style="zoom:50%;" />