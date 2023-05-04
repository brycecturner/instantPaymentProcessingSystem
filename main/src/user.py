from transaction import transaction

class user:

    def __init__(self, user_id, institution_id):
        self.user_id = user_id
        self.institution_id = institution_id
        self.active_transaction = None
    
    def return_balance(self):
        pass
    
    def initiate_interbank_transaction(self):
        self.active_transaction = transaction(self.user_id, self.institution_id)
    
    def cancel_interbank_transaction(self):
        self.active_transaction = None
 
