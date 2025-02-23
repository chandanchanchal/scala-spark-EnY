import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object NYCTaxiInsights {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("NYCTaxiInsights")
      .master("local[*]") // Use appropriate master setting for your environment
      .getOrCreate()

    import spark.implicits._

    // Load the NYC taxi data
    val taxiData: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("path/to/2020_Yellow_Taxi_Trip_Data.csv") // Update with your file path

    // Display schema and a few rows
    taxiData.printSchema()
    taxiData.show(5)

    // Total number of trips
    val totalTrips = taxiData.count()
    println(s"Total number of trips: $totalTrips")

    // Average fare per trip
    val avgFare = taxiData.agg(avg("fare_amount").alias("avg_fare")).first().getAs[Double]("avg_fare")
    println(f"Average fare per trip: $$${avgFare}%.2f")

    // Peak pickup hours
    val peakHours = taxiData
      .withColumn("hour", hour($"tpep_pickup_datetime"))
      .groupBy("hour")
      .count()
      .orderBy(desc("count"))
    println("Peak pickup hours:")
    peakHours.show(5)

    // Busiest pickup locations
    val busiestLocations = taxiData
      .groupBy("PULocationID")
      .count()
      .orderBy(desc("count"))
    println("Busiest pickup locations:")
    busiestLocations.show(5)

    // Trip Distance Analysis
    val distanceStats = taxiData.agg(
      avg("trip_distance").alias("avg_distance"),
      min("trip_distance").alias("min_distance"),
      max("trip_distance").alias("max_distance")
    ).first()
    println(f"Average trip distance: ${distanceStats.getAs[Double]("avg_distance")}%.2f miles")
    println(f"Minimum trip distance: ${distanceStats.getAs[Double]("min_distance")}%.2f miles")
    println(f"Maximum trip distance: ${distanceStats.getAs[Double]("max_distance")}%.2f miles")

    // Fare Distribution Analysis
    val fareDistribution = taxiData
      .select("fare_amount")
      .groupBy("fare_amount")
      .count()
      .orderBy("fare_amount")
    println("Fare distribution:")
    fareDistribution.show(10)

    // Stop Spark session
    spark.stop()
  }
}
