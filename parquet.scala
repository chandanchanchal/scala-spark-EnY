val data = Seq(("James ","","Smith","36636","M",3000),
              ("Michael ","Rose","","40288","M",4000),
              ("Robert ","","Williams","42114","M",4000),
              ("Maria ","Anne","Jones","39192","F",4000),
              ("Jen","Mary","Brown","","F",-1))

val columns = Seq("firstname","middlename","lastname","dob","gender","salary")

import spark.sqlContext.implicits._
val df = data.toDF(columns:_*)

df.write.parquet("/home/ansadmin/data/people.parquet")
parquetDF.createOrReplaceTempView("parqTable")
val parksql = spark.sql("select * from parqTable where salary >= 4000")
parksql.show()
