// Config A: Data Generator with Logical Timestamp Columns
// Generates 500 columns with timestamp logical types (every 50th column)
// Usage: spark-shell -i generate_config_a.scala --conf spark.driver.memory=8g

import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import java.sql.Timestamp
import java.util.UUID

// Create Spark session
val spark = SparkSession.builder()
  .appName("ConfigA-DataGenerator-WithLogicalTypes")
  .getOrCreate()

import spark.implicits._

// ----------------------------------------------------------------------
// Parameters
// ----------------------------------------------------------------------
val numCols = 500
val numPartitions = 10000
val baseTime = LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS)
val zone = java.time.ZoneId.systemDefault()

// S3 output path
val outputPath = "s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/data/raw_config_a_initial"

println(s"🚀 Config A: Generating data with logical timestamp columns")
println(s"   Columns: ${numCols}")
println(s"   Partitions: ${numPartitions}")
println(s"   Output: ${outputPath}")

// Helper functions
def toTimestamp(local: LocalDateTime): Timestamp = Timestamp.valueOf(local)
def toMillis(local: LocalDateTime): Long = local.atZone(zone).toInstant.toEpochMilli

// ----------------------------------------------------------------------
// Define schema dynamically
// ----------------------------------------------------------------------
val fields = collection.mutable.ArrayBuffer[StructField]()

// Add Hudi required fields first
fields += StructField("id", StringType, false)  // Record key
fields += StructField("precombine_field", LongType, false)  // Ordering field
fields += StructField("partition_key", StringType, false)  // Partition field

// Add 500 data columns
(1 to numCols).foreach { i =>
  if (i % 50 == 0) {
    // Every 50th column is a timestamp - alternating between millis and micros
    if ((i / 50) % 2 == 0) {
      // Even: timestamp-millis (stored as LongType)
      fields += StructField(s"ts_millis_$i", LongType, true)
    } else {
      // Odd: timestamp-micros (stored as TimestampType)
      fields += StructField(s"ts_micros_$i", TimestampType, true)
    }
  } else {
    // Regular string columns
    fields += StructField(s"col_$i", StringType, true)
  }
}

val schema = StructType(fields.toSeq)

println(s"📋 Schema created with ${schema.fields.length} fields")
println(s"   Timestamp columns (millis): ${schema.fields.count(f => f.name.startsWith("ts_millis_"))}")
println(s"   Timestamp columns (micros): ${schema.fields.count(f => f.name.startsWith("ts_micros_"))}")
println(s"   String columns: ${schema.fields.count(f => f.name.startsWith("col_"))}")

// ----------------------------------------------------------------------
// Generate data
// ----------------------------------------------------------------------
println(s"📊 Starting data generation...")

val data = spark.sparkContext.parallelize(1 to numPartitions, numPartitions).map { i =>
  val localTs = baseTime.plusSeconds(i)
  val recordTimestamp = toMillis(localTs)

  // Generate UUID for record key
  val recordId = f"record_${i}%05d"

  // Partition key
  val partitionKey = f"partition_${i}%05d"

  // Build values for all columns
  val values = collection.mutable.ArrayBuffer[Any]()

  // Add required fields
  values += recordId
  values += recordTimestamp
  values += partitionKey

  // Add data columns
  (1 to numCols).foreach { colIdx =>
    if (colIdx % 50 == 0) {
      // Timestamp columns with varying values
      if ((colIdx / 50) % 2 == 0) {
        // ts_millis - LongType
        values += toMillis(localTs.plusNanos(colIdx * 1000L))
      } else {
        // ts_micros - TimestampType
        values += toTimestamp(localTs.plusNanos(colIdx * 2000L))
      }
    } else {
      // Regular string value
      values += s"value_${i}_${colIdx}"
    }
  }

  Row.fromSeq(values.toSeq)
}

// ----------------------------------------------------------------------
// Create DataFrame and write to S3
// ----------------------------------------------------------------------
val df = spark.createDataFrame(data, schema)

println(s"✍️  Writing to S3: ${outputPath}")
println(s"   Repartitioning by partition_key...")

df.repartition(numPartitions, $"partition_key")
  .write
  .mode("overwrite")
  .parquet(outputPath)

println(s"✅ Config A: Data generation completed successfully!")
println(s"📍 Location: ${outputPath}")
println(s"📊 Sample schema:")
df.printSchema()
println(s"📊 Sample data:")
df.show(5, truncate = false)

// Stop the spark session
spark.stop()
