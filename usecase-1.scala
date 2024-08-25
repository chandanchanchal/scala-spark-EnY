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
