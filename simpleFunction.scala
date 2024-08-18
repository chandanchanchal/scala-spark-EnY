object simpleFunction{

 def main(args: Array[String] ){
     println("Sum of two integers are :" + functionAdd(9,11))

  }

  def functionAdd(a: Int, b: Int) : Int = {

    var sum:Int = 0
    sum = a+b
    return sum
  
  }

}
