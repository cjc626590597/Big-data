package scala.com.suntek.algorithm.lcs

import com.suntek.algorithm.algorithm.lcs.LCSAlgorithm

/**
  * Created with IntelliJ IDEA.
  * Author: yuanweilin
  * Date: 2020-9-1 18:30
  * Description:
  */
object LCSTest {


  def main(args: Array[String]): Unit = {
    val x = "device1_t1#device2_t2#device3_t3#device4_t4#device5_t5#device6_t6";
    val y = "device1_t1#device2_t2#device7_t3#device4_t4#device4_t4#device6_t6"
   println(LCSAlgorithm.getLCS(x, y))
  }



}
