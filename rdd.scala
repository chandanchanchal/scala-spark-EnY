//Create RDD from parallelize    
val dataSeq = Seq(("Java", 20000), ("Python", 100000), ("Scala", 3000))   
val rdd=spark.sparkContext.parallelize(dataSeq)


//Create RDD from external Data source
val rdd2 = spark.sparkContext.textFile("/path/textFile.txt")

