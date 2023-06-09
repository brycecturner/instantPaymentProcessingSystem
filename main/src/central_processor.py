from pyspark.sql import SparkSession, types
from pyspark.sql.functions import *
import pyspark



spark = (SparkSession
         .builder
         .appName("central_processor_topic_parser")
         .config('spark.jars.packages', "io.delta:delta-core_2.12:2.3.0,io.delta:delta-storage:2.3.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")
         .config('spark.sql.extension', 'io.delta.sql.DeltaSparkSessionExtension')
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .enableHiveSupport()
         .getOrCreate())



spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
checkpoint_dir = "/home/bcturner/instantPaymentProcessingSystem/main/data/central_processor_checkpoints"


input_stream = (
    spark
    .readStream 
    .format('kafka')
    .option('kafka.bootstrap.servers', 'localhost:9092')
    .option('subscribe', 'central_topic')
    .option('earliestOffset', 'latest')
    .option('failOnDataLoss', 'false')
    .load()
    .selectExpr("CAST (value AS STRING)")
    
    #Parse Kafka Message to Find Correct Topic
    .select(
        split(col('value'), ",").getItem(3).alias("instId"),
        "value")
    .withColumn("topic", 
                concat(lit('institutionId_'),col("instId").cast(types.StringType()) )  
               )
    
    #Write to Correct Kafka Topic
    .writeStream
    .format('kafka')
    .outputMode('append')
    .option('kafka.bootstrap.servers', 'localhost:9092')
    .option('checkpointLocation', "/home/bcturner/instantPaymentProcessingSystem/main/data/central_processor_checkpoints/")
    .start()
    .awaitTermination()
)

