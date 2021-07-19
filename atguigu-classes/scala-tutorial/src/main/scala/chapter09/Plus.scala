package chapter09

object Plus {
  def main(args: Array[String]): Unit = {
    implicit class MyRichInt(self: Int){
      def Mymax(x: Int): Int = if(self > x) self else x
      def Mymin(x: Int): Int = if(self < x) self else x
    }

    val x = 3
    println(x.Mymax(5))
  }
}
