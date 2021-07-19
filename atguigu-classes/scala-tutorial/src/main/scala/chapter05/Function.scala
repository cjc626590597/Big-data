package chapter05

import scala.annotation.tailrec

object Function {
  def main(args: Array[String]): Unit = {

    def f(name: String)= println(name)

    f("atguigu")

    var fun = (name: String) => println(name)

    def f1(func: String => Unit) = func("atguigu")

    f1(fun)

    def oneAndTwo(func:(Int,Int) => Int): Int = {
      func(1, 2)
    }

    println(oneAndTwo((_ + _)))
    println(oneAndTwo((_ - _)))

    val fun1 = (i: Int, s: String, c: Char) => {
      if( i == 0 && s =="" && c =='0') false else true
    }

    println(fun1(0, "", '0'))
    println(fun1(0, "", '1'))

    def func(i: Int): String=>(Char=>Boolean) = {
      def f1(s: String): Char=>Boolean = {
        def f2(c: Char): Boolean = {if(i==0 && s=="" && c=='0') false else true}
        f2
      }
      f1
    }

    println(func(0)("")('0'))
    println(func(0)("")('1'))

    def func1(i: Int): String=>(Char=>Boolean) = {
      s => c => {if(i==0 && s=="" && c=='0') false else true}
    }

    println(func1(0)("")('0'))
    println(func1(0)("")('1'))

    def recurse(n: Int): Int ={
      @tailrec
      def tailRecurse(n: Int, curResult: Int): Int = {
        if(n == 0) return curResult
        tailRecurse(n-1, curResult * n)
      }
      tailRecurse(n, 1)
    }

    println(recurse(5))

    def control(a: => Int) = {
      println(s"a = $a")
      println(s"a = $a")
    }
    def control1():Int = 11
    control(control1())

    var n: Int = 1
    while (n <= 5){
      print( n+" " )
      n += 1
    }

    def myWhile(condition: =>Boolean): (=>Unit)=>Unit = {
      op => {
        if (condition) {
          op
          myWhile(condition)(op)
        }
      }
    }

    n = 1
    myWhile (n <= 5){
      print( n+" " )
      n += 1
    }

    def myWhile1(condition: =>Boolean)(op: =>Unit): Unit = {
        if (condition) {
          op
          myWhile(condition)(op)
        }
    }

    n = 1
    myWhile1 (n <= 5){
      print( n+" " )
      n += 1
    }
  }
}
