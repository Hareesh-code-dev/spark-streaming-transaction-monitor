from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, sum as _sum, count
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

spark = SparkSession.builder.appName("TxnStreaming").getOrCreate()

schema = StructType() \
    .add("account_id", StringType()) \
    .add("amount", DoubleType()) \
    .add("timestamp", TimestampType())

df = spark.readStream \
    .schema(schema) \
    .json("/Volumes/workspace/default/data_stream/")

agg = df.withWatermark("timestamp", "2 minutes") \
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("account_id")
    ) \
    .agg(
        _sum("amount").alias("total_amount"),
        count("*").alias("txn_count")
    )

flagged = agg.filter(col("total_amount") > 5000)

query = flagged.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .option("checkpointLocation", "/Volumes/workspace/default/data_stream/_checkpoint") \
    .trigger(availableNow=True) \
    .start()

query.awaitTermination()
