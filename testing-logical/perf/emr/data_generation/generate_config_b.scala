// Config B: Data Generator WITHOUT Logical Timestamp Columns
// Generates 500 columns with ALL string types (no timestamp logical types)
// This is used to certify no performance overhead from the logical type fix
// Usage: spark-shell -i generate_config_b.scala --conf spark.driver.memory=8g

import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import java.util.UUID

// Create Spark session
val spark = SparkSession.builder()
  .appName("ConfigB-DataGenerator-WithoutLogicalTypes")
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
val outputPath = "s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/data/raw_config_b_initial"

println(s"🚀 Config B: Generating data WITHOUT logical timestamp columns")
println(s"   Columns: ${numCols} (all strings)")
println(s"   Partitions: ${numPartitions}")
println(s"   Output: ${outputPath}")

// Helper function
def toMillis(local: LocalDateTime): Long = local.atZone(zone).toInstant.toEpochMilli

// ----------------------------------------------------------------------
// Define schema dynamically
// ----------------------------------------------------------------------
val fields = collection.mutable.ArrayBuffer[StructField]()

// Add Hudi required fields first
fields += StructField("id", StringType, false)  // Record key
fields += StructField("precombine_field", LongType, false)  // Ordering field
fields += StructField("partition_key", StringType, false)  // Partition field

// Add 500 data columns - ALL STRING TYPES (no logical timestamp types)
(1 to numCols).foreach { i =>
  // All columns are string type, even those at positions that would be timestamps in Config A
  fields += StructField(s"col_$i", StringType, true)
}

val schema = StructType(fields.toSeq)

println(s"📋 Schema created with ${schema.fields.length} fields")
println(s"   String columns: ${schema.fields.count(f => f.name.startsWith("col_"))}")
println(s"   NO logical timestamp columns (control group for performance comparison)")

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

  // Add data columns - all as strings
  (1 to numCols).foreach { colIdx =>
    // For positions that would be timestamps in Config A, use string representation
    // This maintains the same data structure but without logical types
    if (colIdx % 50 == 0) {
      // At timestamp positions, store timestamp as string
      if ((colIdx / 50) % 2 == 0) {
        values += s"ts_millis_${recordTimestamp + colIdx}"
      } else {
        values += s"ts_micros_${recordTimestamp + colIdx * 2}"
      }
    } else {
      // Regular string value (same as Config A)
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

println(s"✅ Config B: Data generation completed successfully!")
println(s"📍 Location: ${outputPath}")
println(s"📊 Sample schema:")
df.printSchema()
println(s"📊 Sample data:")
df.show(5, truncate = false)

// Stop the spark session
spark.stop()
