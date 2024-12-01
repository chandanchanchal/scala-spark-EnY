//Create RDD from parallelize    
val dataSeq = Seq(("Java", 20000), ("Python", 100000), ("Scala", 3000))   
val rdd=spark.sparkContext.parallelize(dataSeq)


//Create RDD from external Data source
val rdd2 = spark.sparkContext.textFile("/path/textFile.txt")

echo -e "1,John,25\n2,Jane,30\n3,Smith,28\n4,Emily,35\n5,Alice,22" > data.csv

val rawData = sc.textFile("data.csv")
rawData.foreach(println)

val mappedData = rawData.map(line => {
  val parts = line.split(",")
  (parts(0).toInt, parts(1), parts(2).toInt)
})
mappedData.foreach(println)


val filteredData = mappedData.filter { case (_, _, age) => age < 30 }
filteredData.foreach(println)

val flatMappedData = mappedData.flatMap { case (_, name, _) => name }
flatMappedData.foreach(println)


echo -e "1,Male\n2,Female\n3,Male\n4,Female\n5,Female" > genders.csv


val genderData = sc.textFile("genders.csv")
val genderMapping = genderData.map(line => {
  val parts = line.split(",")
  (parts(0).toInt, parts(1))
})

val joinedData = mappedData.map { case (id, _, age) => (id, age) }
  .join(genderMapping)
  .map { case (_, (age, gender)) => (gender, age) }

val totalAgeByGender = joinedData.reduceByKey(_ + _)
totalAgeByGender.foreach(println)


val groupedData = joinedData.groupByKey()
groupedData.foreach { case (gender, ages) =>
  println(s"$gender: ${ages.mkString(", ")}")
}


val sortedData = mappedData.sortBy { case (_, _, age) => age }
sortedData.foreach(println)


val additionalData = sc.parallelize(Seq((6, "Bob", 40), (7, "Tom", 29)))
val combinedData = mappedData.union(additionalData)
combinedData.foreach(println)


val distinctNames = mappedData.map { case (_, name, _) => name }.distinct()
distinctNames.foreach(println)


val sampledData = mappedData.sample(withReplacement = false, fraction = 0.5)
sampledData.foreach(println)




#######################################---for set operation-----
val dataset1 = spark.sparkContext.parallelize(List(1, 2, 3, 4, 5))
val dataset2 = spark.sparkContext.parallelize(List(4, 5, 6, 7, 8))

val unionRDD = dataset1.union(dataset2)
println("Union of the two datasets:")
unionRDD.collect().foreach(println)


val distinctUnionRDD = unionRDD.distinct()
println("Distinct Union of the two datasets:")
distinctUnionRDD.collect().foreach(println)


val intersectionRDD = dataset1.intersection(dataset2)
println("Intersection of the two datasets:")
intersectionRDD.collect().foreach(println)


val subtractionRDD = dataset1.subtract(dataset2)
println("Elements in dataset1 but not in dataset2:")
subtractionRDD.collect().foreach(println)

val cartesianRDD = dataset1.cartesian(dataset2)
println("Cartesian product of the two datasets:")
cartesianRDD.collect().foreach(println)


val filteredRDD = unionRDD.filter(_ % 2 == 0)
println("Even numbers from the union of the two datasets:")
filteredRDD.collect().foreach(println)


println(s"Count of elements in Union: ${unionRDD.count()}")
println(s"Count of elements in Intersection: ${intersectionRDD.count()}")
println(s"Count of elements in Subtraction: ${subtractionRDD.count()}")








