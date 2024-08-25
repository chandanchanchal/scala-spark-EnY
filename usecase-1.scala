import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Encoder, Encoders, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{avg, sum}
import java.sql.Date


def toDS[T <: Product: Encoder](df: DataFrame): Dataset[T] = df.as[T]

##Create Datasets
final case class Person(
    personId: Int,
    firstName: String,
    lastName: String)
  final case class Sales(
    date: Date,
    personId: Int,
    customerName: String,
    amountDollars: Double)

##This is our data which we’ll create using Seq types. We'll use two of them, one for people, and the other a set of sales data.
val personData: Seq[Row] = Seq(
    Row(1, "Eric", "Tome"),
    Row(2, "Jennifer", "C"),
    Row(3, "Cara", "Rae")
  )
  val salesData: Seq[Row] = Seq(
    Row(new Date(1577858400000L), 1, "Third Bank", 100.29),
    Row(new Date(1585717200000L), 3, "Pet's Paradise", 1233451.33),
    Row(new Date(1585717200000L), 2, "Small Shoes", 4543.35),
    Row(new Date(1593579600000L), 1, "PaperCo", 84990.15),
    Row(new Date(1601528400000L), 1, "Disco Balls'r'us", 504.00),
    Row(new Date(1601528400000L), 2, "Big Shovels", 9.99)
  )
##Using Spark, we can read data from Scala Seq objects. The following code will create an StructType object from the case classes defined above. Then we have a function getDSFromSeq that takes parameters data and schema. We then use Spark to read our Seq objects while strongly typing them.
private val personSchema: StructType = Encoders.product[Person].schema
  private val salesSchema: StructType  = Encoders.product[Sales].schema
  def getDSFromSeq[T <: Product: Encoder](data: Seq[Row], schema: StructType) =
    spark
      .createDataFrame(
        spark.sparkContext.parallelize(data),
        schema
      ).as[T]
  val personDS: Dataset[Person] = getDSFromSeq[Person](personData, personSchema)
  val salesDS: Dataset[Sales] = getDSFromSeq[Sales](salesData, salesSchema)

## Validate data output by calling personDS.show() as well as salesDS.show()
personDS.show()
salesDS.show()

##Filtering
personDS.filter(r => r.firstName.contains("Eric"))
salesDS.filter(r => r.personId.equals(1))

# certain column name we need to change

df.withColumnRenamed("col1", "newcol1")
        .withColumnRenamed("col2", "newcol2")
        .withColumnRenamed("col3", "newcol3")
        .withColumnRenamed("col4", "newcol4")
        ...
        .withColumnRenamed("coln", "newcoln")

#However, when modifying a large number of columns there are more elegant solutions.

#Create a case class that defines how your final set of data should look.
#Create a function that returns a Map[String, String] where the first string is the current column name, and the second is the new name.
#Create a function that takes that Map and folds over the input Dataset. The function within the fold is withColumnRenamed which takes the values from the Map for the current column name and a new name. A new Dataset is returned type with your final case class.
