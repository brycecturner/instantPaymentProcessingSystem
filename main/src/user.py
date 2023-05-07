from confluent_kafka import Producer 

from transaction import transaction

class user:

    def __init__(self, user_id, institution_id):
        self.user_id = user_id
        self.institution_id = institution_id
        self.active_transaction = None
    
    def return_balance(self):
        pass
    
    """  Functions for Sending Interbank Transfers """
    
    def initiate_interbank_transaction(self):
        self.active_transaction = transaction(self.user_id, self.institution_id)
    
    def cancel_interbank_transaction(self):
        self.active_transaction = None
    
    def accept_transaction_details(self):
        if self.active_transaction is not None:
            self.active_transaction.set_kafka_message()
    
    def produce_message_to_kafka(self):
        if self.active_transaction.kafka_message is not None:
            kafka_producer=Producer({'bootstrap.servers':'localhost:9092'})
            kafka_producer.produce('central_topic', self.active_transaction.kafka_message)
            kafka_producer.flush()