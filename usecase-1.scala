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
