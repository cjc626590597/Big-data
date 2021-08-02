package com.suntek.algorithm.process.lcss

import org.slf4j.{Logger, LoggerFactory}

import java.io.{BufferedInputStream, BufferedReader, InputStream, InputStreamReader}
import java.util.concurrent.CountDownLatch


class ThreadDistribution(command: String, latch: CountDownLatch) extends Runnable {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)


  override def run(): Unit ={

    logger.info("exec shell command: " + command)

    val process = Runtime.getRuntime.exec(command)

    /*为"错误输出流"单独开一个线程读取之,否则会造成标准输出流的阻塞*//*为"错误输出流"单独开一个线程读取之,否则会造成标准输出流的阻塞*/
    val thread = new Thread(new InputErrorStreamRunnable(process.getErrorStream))

    thread.start()
    //标准输出
    val bufferedInputStream = new BufferedInputStream(process.getInputStream)

    val buf = new BufferedReader(new InputStreamReader(bufferedInputStream))

    // command log

    var line = ""

    var flag: Boolean = true
    while (flag) {
      line = buf.readLine()
      if (line == null) {
        flag = false
      } else {
        logger.info(line)
      }
    }

    process.waitFor()
    val exitValue = process.exitValue
    if (exitValue == 0){
      logger.info("shell command success")
    }else {
      logger.error("shell command failed")
    }

    if (buf != null) {
      buf.close()
    }
    if (bufferedInputStream != null) {
      bufferedInputStream.close()
    }
    process.destroy()
    latch.countDown()
  }
}

class InputErrorStreamRunnable(io: InputStream) extends Runnable {

  override def run(): Unit = {

    val bReader = new BufferedReader(new InputStreamReader(new BufferedInputStream(io), "UTF-8"))

    var line = ""
    var lineNum = 0L
    try {
      var flag: Boolean = true
      while (flag) {
        line = bReader.readLine()
        if (line == null) {
          flag = false
        } else {
          lineNum += 1L
        }
      }
    } finally {
      if (bReader != null) {
        bReader.close()
      }
    }
  }
}
