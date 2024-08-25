spark-shell --packages com.databricks:spark-xml_2.12:0.18.0
import com.databricks.spark.xml._

val df = spark.read.option("rowTag", "book").xml("/home/ansadmin/data/scala-spark-EnY/Data/books.xml")
  
  
val selectedData = df.select("author", "_id")
selectedData.write.option("rootTag", "books").option("rowTag", "book").xml("/home/ansadmin/data/scala-spark-EnY/Data/newbooks.xml")

import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType}

val customSchema = StructType(Array(
  StructField("_id", StringType, nullable = true),
  StructField("author", StringType, nullable = true),
  StructField("description", StringType, nullable = true),
  StructField("genre", StringType, nullable = true),
  StructField("price", DoubleType, nullable = true),
  StructField("publish_date", StringType, nullable = true),
  StructField("title", StringType, nullable = true)))


val df = spark.read.option("rowTag", "book").schema(customSchema).xml("/home/ansadmin/data/scala-spark-EnY/Data/books.xml")

val selectedData = df.select("author", "_id")
selectedData.write.option("rootTag", "books").option("rowTag", "book").xml("/home/ansadmin/data/scala-spark-EnY/Data/newbooks1.xml")
