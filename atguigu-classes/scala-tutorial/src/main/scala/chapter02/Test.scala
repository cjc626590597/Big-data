package chapter02

import scala.io.StdIn

object Test {
  def main(args: Array[String]): Unit = {
    var age = 20
    var name: String = "Alice"
    printf("%d岁的%s在尚硅谷学习", age, name)

    println(s"${age}岁的${name}在尚硅谷学习")

    var a = 2.345
    println(f"值为${a}%2.2f")

    val s =
      """
        |select *
        |from
        |student
        |""".stripMargin
    println(s)

    println("请输入你的大名")
    val name1 = StdIn.readLine()
    println("请输入你的芳龄")
    val age1 = StdIn.readInt()
    println(s"欢迎${age1}岁的${name1}来到尚硅谷学习")

  }
}
