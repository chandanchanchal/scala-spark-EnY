

// Create data
val columns = Seq("language","users_count")
val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))

val dfFromRdd1 = rdd.toDF()
dfFromRdd1.printSchema()
dfFromRdd1.show()
