package chapter02

import java.io.{File, PrintWriter}

import scala.io.Source

object FileIO {
  def main(args: Array[String]): Unit = {
    Source.fromFile("scala-tutorial/src/main/resources/test.txt").foreach(print)

    val writer = new PrintWriter(new File("scala-tutorial/src/main/resources/output.txt"))
    writer.write("hello scala")
    writer.close()

  }
}
