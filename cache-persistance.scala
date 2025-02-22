// Create sparkSession and apply cache() on DataFrame
val spark:SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("ccspark")
    .getOrCreate()

import spark.implicits._
val columns = Seq("Seqno","Quote")
val data = Seq(("1", "Be the change that you wish to see in the world"),
    ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
    ("3", "The purpose of our lives is to be happy."))
val df = data.toDF(columns:_*)

val dfCache = df.cache()
dfCache.show(false)


###################################################################

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

// Create a SparkSession
val spark: SparkSession = SparkSession.builder()
  .master("local[1]")
  .appName("PersistenceExample")
  .getOrCreate()

import spark.implicits._

// Define the columns and data
val columns = Seq("Seqno", "Quote")
val data = Seq(
  ("1", "Be the change that you wish to see in the world"),
  ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
  ("3", "The purpose of our lives is to be happy.")
)

// Create a DataFrame
val df = data.toDF(columns: _*)

// 1. MEMORY_ONLY (default cache)
// This stores the DataFrame only in memory. It's fast but may cause issues if the DataFrame is too large.
val dfCache = df.cache()
dfCache.show(false)

// 2. MEMORY_AND_DISK
// If the DataFrame does not fit entirely in memory, Spark will store the remaining partitions on disk.
val dfMemDisk = df.persist(StorageLevel.MEMORY_AND_DISK)
dfMemDisk.show(false)

// 3. DISK_ONLY
// The DataFrame will be stored only on disk. This can be useful when memory is a constraint.
val dfDisk = df.persist(StorageLevel.DISK_ONLY)
dfDisk.show(false)

// 4. MEMORY_ONLY_SER (Serialized)
// Storing data in a serialized format can reduce memory usage if data is large, though it may increase CPU overhead.
val dfCacheSer = df.persist(StorageLevel.MEMORY_ONLY_SER)
dfCacheSer.show(false)

// 5. MEMORY_AND_DISK_SER (Serialized)
// This uses serialization while keeping as many partitions in memory as possible and spilling to disk if necessary.
val dfMemDiskSer = df.persist(StorageLevel.MEMORY_AND_DISK_SER)
dfMemDiskSer.show(false)

// It is good practice to unpersist DataFrames when they are no longer needed
dfCache.unpersist()
dfMemDisk.unpersist()
dfDisk.unpersist()
dfCacheSer.unpersist()
dfMemDiskSer.unpersist()

// Stop the SparkSession
spark.stop()

