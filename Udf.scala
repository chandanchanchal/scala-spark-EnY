import spark.implicits._
val columns = Seq("Seqno","Quote")
val data = Seq(("1", "Be the change that you wish to see in the world"),
    ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
    ("3", "The purpose of our lives is to be happy.")
  )
val df = data.toDF(columns:_*)
df.show(false)

val convertCase =  (strQuote:String) => {
    val arr = strQuote.split(" ")
    arr.map(f=>  f.substring(0,1).toUpperCase + f.substring(1,f.length)).mkString(" ")
}

val convertUDF = udf(convertCase)
//Using with DataFrame
df.select(col("Seqno"), 
    convertUDF(col("Quote")).as("Quote") ).show(false)
