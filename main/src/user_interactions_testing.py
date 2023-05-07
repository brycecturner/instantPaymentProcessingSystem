from transaction import transaction
from user import user
from faker import Faker
import time

fake = Faker()

for i in range(10):
    receiving_inst = '0001'
    receiving_user = fake.random_int(min=1, max=100)
    dollar_value = fake.random_int(min=1, max=1000)

    tmp = user('0005', '00003')
    tmp.initiate_interbank_transaction()
    tmp.active_transaction.set_recipient_details(receiving_inst,receiving_user)
    tmp.active_transaction.set_dollar_value(dollar_value)
    tmp.accept_transaction_details()
    tmp.produce_message_to_kafka()

    time.sleep(5)
