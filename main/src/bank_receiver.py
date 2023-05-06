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


input_stream = (
    spark
    .readStream 
    .format('kafka')
    .option('kafka.bootstrap.servers', 'localhost:9092')
    .option('subscribe', f'topic_{institutionId}')
    .option('earliestOffset', 'latest')
    .load()
    .selectExpr("CAST (value AS STRING)", 
                "CAST (timestamp AS STRING)")
    #Parse Kafka Message
    .select(
        split(col('value'), ",").getItem(0).alias("sendingTimestamp"),
        split(col('value'), ",").getItem(1).alias("sendingInstitutionId"),
        split(col('value'), ",").getItem(2).alias("sendingUserId"),
        split(col('value'), ",").getItem(3).alias("receivingInstitutionId"),
        split(col('value'), ",").getItem(4).alias("receivingUserId"),
        split(col('value'), ",").getItem(5).alias("dollarValue"),
        unix_timestamp(col('timestamp')).alias('kafkaTimestamp'))
    #Convert Sending Info to Note
    .withColumn('note',
             concat(lit('Sent via IPPS from '),
                    col("sendingInstitutionId"),
                    lit(' -- '), 
                    col("sendingUserId"),
                    lit(' at '),
                    col('sendingTimestamp')
                   ).alias('sender') 
              )
    #Rename Rename ot make 
         .select(
             col('receivingInstitutionId').alias('institutionId'), 
             col("receivingUserId").alias("userId"), 
             col("dollarValue").alias('netChange'), 
             col('kafkaTimestamp').cast(types.LongType()).alias('timestamp'),
             "note"
    )
    #Append to Delta Data
    .writeStream
    .format('delta')
    .outputMode('append')
    .option('checkpointLocation', "/home/bcturner/instantPaymentProcessingSystem/main/data/dummy_delta_data")
    .start("/home/bcturner/instantPaymentProcessingSystem/main/data/dummy_delta_data")
    .awaitTermination()
   
)
