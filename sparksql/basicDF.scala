import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.functions.array_contains

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



val dfFromRdd1 = rdd.toDF()
dfFromRdd1.printSchema()
dfFromRdd1.show()
val dfFromRdd1 = rdd.toDF("language","users_count")
dfFromRdd1.printSchema()
dfFromRdd1.show()

val df = spark.read.csv("/home/ansadmin/data/scala-spark-EnY/Data/zipcodes.csv")

val df3 = spark.read.option("header",true).csv("/home/ansadmin/data/scala-spark-EnY/Data/zipcodes.csv")
df3.printSchema()

val options = Map("infereSchema"->"true","delimiter"->",","header"->"true")
val df3 = spark.read.options(options).csv("/home/ansadmin/data/scala-spark-EnY/Data/zipcodes.csv")
df3.printSchema()


/ Import types
import org.apache.spark.sql.types._

// Read with custom schema
val schema = new StructType()
      .add("RecordNumber",IntegerType,true)
      .add("Zipcode",IntegerType,true)
      .add("ZipCodeType",StringType,true)
      .add("City",StringType,true)
      .add("State",StringType,true)
      .add("LocationType",StringType,true)
      .add("Lat",DoubleType,true)
      .add("Long",DoubleType,true)
      .add("Xaxis",IntegerType,true)
      .add("Yaxis",DoubleType,true)
      .add("Zaxis",DoubleType,true)
      .add("WorldRegion",StringType,true)
      .add("Country",StringType,true)
      .add("LocationText",StringType,true)
      .add("Location",StringType,true)
      .add("Decommisioned",BooleanType,true)
      .add("TaxReturnsFiled",StringType,true)
      .add("EstimatedPopulation",IntegerType,true)
      .add("TotalWages",IntegerType,true)
      .add("Notes",StringType,true)

// Read CSV file with custom schema
val df_with_schema = spark.read.format("csv")
      .option("header", "true")
      .schema(schema)
      .load("zipcodes.csv")
df_with_schema.printSchema()
df_with_schema.show(false)

 val arrayStructureData = Seq(
    Row(Row("James","","Smith"),List("Java","Scala","C++"),"OH","M"),
    Row(Row("Anna","Rose",""),List("Spark","Java","C++"),"NY","F"),
    Row(Row("Julia","","Williams"),List("CSharp","VB"),"OH","F"),
    Row(Row("Maria","Anne","Jones"),List("CSharp","VB"),"NY","M"),
    Row(Row("Jen","Mary","Brown"),List("CSharp","VB"),"NY","M"),
    Row(Row("Mike","Mary","Williams"),List("Python","VB"),"OH","M")
  )

  val arrayStructureSchema = new StructType()
    .add("name",new StructType()
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType))
    .add("languages", ArrayType(StringType))
    .add("state", StringType)
    .add("gender", StringType)

  val df = spark.createDataFrame(
    spark.sparkContext.parallelize(arrayStructureData),arrayStructureSchema)
  df.printSchema()
  df.show()

 df.where(df("state") === "OH")
    .show(false)
