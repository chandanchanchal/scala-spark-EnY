

// Create data
val columns = Seq("language","users_count")
val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))

val dfFromRdd1 = rdd.toDF()
dfFromRdd1.printSchema()
dfFromRdd1.show()

val dfFromRdd1 = rdd.toDF("language","users_count")
dfFromRdd1.printSchema()
dfFromRdd1.show()

// Additional Imports
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.Row

// Create StructType Schema
val schema = StructType( Array(
                 StructField("language", StringType,true),
                 StructField("users", StringType,true)
             ))

// Use map() transformation to get Row type
val rowRDD = rdd.map(attributes => Row(attributes._1, attributes._2))
val dfFromRDD3 = spark.createDataFrame(rowRDD,schema)
