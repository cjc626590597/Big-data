package com.suntek.algorithm.algorithm.lcs

import org.apache.commons.lang3.StringUtils

import scala.Array.ofDim
import scala.collection.mutable
import scala.math.max

import scala.collection.JavaConverters._

/**
  * Created with IntelliJ IDEA.
  * Author: yuanweilin
  * Date: 2020-9-1 15:47
  * Description:最长公共子序列算法
  */
object LCSAlgorithm {

      def getLCSFromStringArray(arrayX:Array[String], arrayY:Array[String] , delimited: String ="#"): (String, Int) ={
        val x_length = arrayX.length
        val y_length = arrayY.length
        val lcs_dim:Array[Array[Int]] = ofDim(x_length + 1, y_length + 1)

        for(i <- 0 to lcs_dim(0).length -1){
          //第一行填充0
          lcs_dim(0)(i) = 0
        }

        for(j <- 0 to lcs_dim.length -1){
          //第一列填充0
          lcs_dim(j)(0) = 0
        }

        for(m <- 1 to lcs_dim.length -1){
          for(n <- 1 to lcs_dim(m).length -1 ){
              if(arrayX( m -1) == arrayY( n - 1)){
                lcs_dim(m)(n) = lcs_dim(m-1)(n-1)+1
              }else{
                lcs_dim(m)(n) = max(lcs_dim(m-1)(n), lcs_dim(m)(n-1))
              }
          }
        }

        var i = arrayX.length -1
        var j = arrayY.length -1
        var stack = mutable.Stack[String]()
        while( i >= 0 && j >=0){
          if(arrayX(i) == arrayY(j)){
            //输出相同的字符
            stack.push(arrayX(i))
            i -= 1
            j -= 1
          }else{
            if(lcs_dim( i+1 )(j) > lcs_dim(i)(j + 1)){
              //按列查找
              j -= 1
            }else{
              //按行查找
              i -= 1
            }
          }
        }
        var lcsStackList = mutable.ListBuffer[String]();
        while ( !stack.isEmpty){
          lcsStackList+=(stack.pop())
        }
        (StringUtils.join(lcsStackList.toList.asJava, delimited), lcs_dim(lcs_dim.length -1 )(lcs_dim(0).length -1))

      }

    def  getLCS(stringX: String, stringY: String , delimited :String = "#"): (String, Int) ={
      if(StringUtils.isBlank(stringX) || StringUtils.isBlank(stringY))
        ("", 0)
      else
        getLCSFromStringArray(stringX.split(delimited), stringY.split(delimited))
  }
}
