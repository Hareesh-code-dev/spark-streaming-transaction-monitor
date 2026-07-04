from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, sum as _sum, count
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

spark = SparkSession.builder.appName("TxnStreaming").getOrCreate()

schema = StructType() \
    .add("account_id", StringType()) \
    .add("amount", DoubleType()) \
    .add("timestamp", TimestampType())

# Read the simulated transaction stream
df = spark.readStream \
    .schema(schema) \
    .json("/Volumes/workspace/default/data_stream/")

# Windowed aggregation with watermarking to handle late-arriving data
agg = df.withWatermark("timestamp", "2 minutes") \
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("account_id")
    ) \
    .agg(
        _sum("amount").alias("total_amount"),
        count("*").alias("txn_count")
    )

# Flag accounts with high transaction volume in a 1-minute window (velocity-based fraud signal)
flagged = agg.filter(col("total_amount") > 5000)

# Write flagged results to a Delta table
delta_output_path = "/Volumes/workspace/default/data_stream/delta_output"
checkpoint_path = "/Volumes/workspace/default/data_stream/_checkpoint_delta"

query = flagged.writeStream \
    .outputMode("append") \
    .format("delta") \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(availableNow=True) \
    .start(delta_output_path)

query.awaitTermination()

# Verify output
df_check = spark.read.format("delta").load(delta_output_path)
display(df_check)
