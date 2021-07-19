package chapter06

object Trait_test {
  def main(args: Array[String]): Unit = {
    println(new MyBall().printDef)
  }
}

trait Ball{
  def printDef(): String = {
    "ball"
  }
}

trait Color extends Ball{
  override def printDef(): String = {
    "red " + super.printDef()
  }
}

trait Category extends Ball{
  override def printDef(): String = {
    "foot " + super.printDef()
  }
}

class MyBall extends Category with Color{
  override def printDef(): String = {
    "I am playing "+ super.printDef()
  }
}
