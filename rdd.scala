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




