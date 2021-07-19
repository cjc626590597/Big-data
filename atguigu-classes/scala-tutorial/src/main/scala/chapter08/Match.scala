package chapter08

import chapter06.Student

object Match {
  def main(args: Array[String]): Unit = {
    val x = 3
    val y = x match {
      case 1 => 'a'
      case 2 => 'b'
      case 3 => 'c'
      case _ => 's'
    }
    println(y)

    val a = 3
    val b = 4
    def charOperator(op: Char): Int  = {
      op match {
        case '+' => a + b
        case '-' => a - b
        case '*' => a * b
        case '/' => a / b
        case _ => -1
      }
    }
    println(charOperator('*'))

    for(arr <- List(
      Array(0),
      Array(1,2),
      Array(1,2,3),
      Array(1,2,3,4),
      Array(-1)
    )){
      val result = arr match {
        case Array(0) => 0
        case Array(1,2) => "Array(1,2)"
        case Array(1,_*) => "Array(1,_*)"
        case _ => "something else"
      }
      println(result)
    }

    val stu = new Student("alice", 18)
    val result = stu match {
      case Student("alice", 18) => "alice, 18"
      case _ => "else"
    }
    println(result)
  }
}

class Student(val name: String,val age: Int)

object Student{
  def apply(name: String, age: Int): Student = new Student(name, age)

  def unapply(stu: Student): Option[(String, Int)] = {
    if(stu == null){
      None
    }else{
      Some(stu.name, stu.age)
    }
  }
}