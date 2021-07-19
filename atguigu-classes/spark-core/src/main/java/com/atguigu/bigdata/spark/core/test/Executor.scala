package com.atguigu.bigdata.spark.core.test

import java.io.{ObjectInput, ObjectInputStream}
import java.net.ServerSocket

object Executor {
  def main(args: Array[String]): Unit = {
    val server = new ServerSocket(8888)

    println("服务器启动")
    val client = server.accept()
    val in = client.getInputStream()
    val objIn = new ObjectInputStream(in)

    val task:Task = objIn.readObject().asInstanceOf[Task]
    val list = task.compute()

    println("接收到" + list)

    in.close()
    client.close()
    server.close()
  }
}
