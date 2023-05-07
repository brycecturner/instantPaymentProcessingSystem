from confluent_kafka import Producer 
from transaction import transaction

from pyspark.sql import SparkSession, types
from pyspark.sql.functions import *
import pyspark
from delta import *
import datetime

class user:

    def __init__(self, user_id, institution_id):
        self.user_id = user_id
        self.institution_id = institution_id
        self.active_transaction = None

        self.spark = (SparkSession
                 .builder
                 .appName(f"sparkSession_{self.institution_id}_{self.user_id}")
                 .config('spark.jars.packages', "io.delta:delta-core_2.12:2.3.0,io.delta:delta-storage:2.3.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")
                 .config('spark.sql.extension', 'io.delta.sql.DeltaSparkSessionExtension')
                 .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                 .enableHiveSupport()
                 .getOrCreate())

        
    def return_balance(self):

        current_balance  = (
            self.spark.read
            .format('delta')
            .load(f"/home/bcturner/instantPaymentProcessingSystem/main/data/institutionId_{self.institution_id}")
            .filter(col('userId')==self.user_id)
            .agg(sum('netChange').alias('currentBalance'))
            .collect()[0][0]
        )

        return current_balance
    
    """  Functions for Sending Interbank Transfers """
    
    def initiate_interbank_transaction(self):
        self.active_transaction = transaction(self.user_id, self.institution_id)
    
    def cancel_interbank_transaction(self):
        self.active_transaction = None
    
    def accept_transaction_details(self):
        current_balance = self.return_balance()

        if current_balance < self.active_transaction.dollar_value:
            print("you do not have enough money in your account to make this transaction")
            return None
        
        if self.active_transaction is not None:
            self.active_transaction.set_kafka_message()
    
    def produce_message_to_kafka(self):
        
        if self.active_transaction.kafka_message is not None:
            kafka_producer=Producer({'bootstrap.servers':'localhost:9092'})
            kafka_producer.produce('central_topic', self.active_transaction.kafka_message)
            kafka_producer.flush()

            single_row = [(f"{self.institution_id}", f"{self.user_id}", f"-{self.active_transaction.dollar_value}",
                           (datetime.datetime.utcnow().timestamp()),
                           f'sent via IPSS to {self.active_transaction.recipient_institution_id}-{self.active_transaction.recipient_user_id}')]

            ( self.spark.createDataFrame(single_row, ['institutionId', 'userId', 'netChange', 'timestamp', 'note'])
                  .withColumn('timestamp', col('timestamp').cast(types.LongType()))
                  .write
                  .format('delta')
                  .mode('append')
                  .option('checkpointLocation', f"/home/bcturner/instantPaymentProcessingSystem/main/data/institutionId_{self.institution_id}")
                  .save(f"/home/bcturner/instantPaymentProcessingSystem/main/data/institutionId_{self.institution_id}")
            )

            self.active_transaction = None
