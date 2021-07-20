package com.atguigu.bigdata.spark.core.test

import java.io.ObjectOutputStream
import java.net.Socket

object Driver {
  def main(args: Array[String]): Unit = {
    val client = new Socket("localhost",8888)

    val out = client.getOutputStream()
    val objOut = new ObjectOutputStream(out)

    val task = new Task()
    objOut.writeObject(task)
    println("数值发送成功")

    objOut.flush()
    objOut.close()
    client.close()
  }
}
