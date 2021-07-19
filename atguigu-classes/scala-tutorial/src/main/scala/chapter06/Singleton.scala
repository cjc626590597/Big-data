package chapter06

object Singleton {
  def main(args: Array[String]): Unit = {
    val stu: Student = Student.getInstance();
    stu.printDef()
  }
}

class Student private(name: String, age: Int ){
  def printDef(): Unit = {
    println(s"name = $name, age = $age")
  }
}

object Student{
  private var student:Student = _
  def getInstance(): Student = {
    if(student == null){
      student = new Student("Alice", 18)
    }
    return student
  }
}