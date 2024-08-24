 val rdd = spark.sparkContext.parallelize(
      List("Germany India USA","USA India Russia","India Brazil Canada China")
    )
 val wordsRdd = rdd.flatMap(_.split(" "))
 val pairRDD = wordsRdd.map(f=>(f,1))
 pairRDD.foreach(println)

//Applying distinct()
pairRDD.distinct().foreach(println)
//SortBykey on pairRDD
println("sort by key ==>>")
val sortRDD = pairRDD.sortByKey()
sortRDD.foreach(println)

val sortRDD = pairRDD.sortByKey()
wordCount.foreach(println)
