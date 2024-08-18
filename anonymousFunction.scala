object anonymousFunction {

 def main(args: Array[String] ){

   var myfunc = (str1:String, str2:String)  =>  str1 + str2

   var myfunc2 = (_:String) + (_:String)  

   println(myfunc("scala "," Learning"))
 
 }

}

