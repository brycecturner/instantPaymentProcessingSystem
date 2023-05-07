from pyspark.sql import SparkSession, types
from pyspark.sql.functions import *
import pyspark
from delta.tables import *
from delta import *

spark = (SparkSession
         .builder
         .appName("read_from_institution_topic")
         .config('spark.jars.packages', "io.delta:delta-core_2.12:2.3.0,io.delta:delta-storage:2.3.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")
         .config('spark.sql.extension', 'io.delta.sql.DeltaSparkSessionExtension')
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .enableHiveSupport()
         .getOrCreate())


spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")


institutionId = "0001"


input_stream = (
    spark
    .readStream
    .format('delta')
    .load(f"/home/bcturner/instantPaymentProcessingSystem/main/data/institutionId_{institutionId}")
    .withColumn("netChange",  col("netChange").cast('int'))
    .withColumn("timestamp", col("timestamp").cast('timestamp'))
    .groupBy("userId")
    .agg(sum('netChange').alias('currentBalance'),
         max('timestamp').alias('maxTimeStamp'))
    .sort('currentBalance')
)


(input_stream
   .withWatermark('maxTimeStamp', '1 minutes')
   .writeStream.format('console')
   .outputMode('complete')
   .trigger(processingTime='30 seconds')
   .start()
   .awaitTermination())
