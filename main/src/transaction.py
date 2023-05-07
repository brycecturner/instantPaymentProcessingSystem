import datetime

class transaction:
    """
    Class to house the information contained in a single bank-to-bank transaction
    """
    
    def __init__(self, user_id, institution_id):

        self.sender_user_id=user_id
        self.sender_institution_id = institution_id

        #Setting unknown key values to None so that they must be explicitly set
        self.recipient_user_id=None
        self.recipient_institution_id=None
        self.dollar_value=None
        self.kafka_message=None

    def set_recipient_details(self, user_id, institution_id):
        self.recipient_user_id=user_id
        self.recipient_institution_id = institution_id

    def set_dollar_value(self, dollar_value):
        self.dollar_value=dollar_value


    def generate_kafka_message(self) -> str:
        """ 
        Generates the kafka message in the proper error. 
        Conducts a check that none of the required fields are blank"""
        
        key_values = [self.sender_institution_id, self.sender_user_id, self.recipient_institution_id, self.recipient_user_id, self.dollar_value]
        
        if any(v is None for v in key_values):
            raise Exception("Some of the critical fields are blank")
        else:
            return ",".join(str(i) for i in key_values)
        
    def set_kafka_message(self):
        
        self.kafka_message=str(datetime.datetime.utcnow().timestamp()) + "," + self.generate_kafka_message()
    
