from user import *


#Initiate User Accounts that can send and recieve messages
# Doing before demo output messages because of the verbose info on
# ... spark initiations
s_user = user(3, '0001')
r_user = user(4, '0001')

dollar_value = 5

print("="*100)
print("DEMO OUTPUT BELOW")
print("="*100)


s_user.initiate_interbank_transaction()
s_user.active_transaction.set_recipient_details('0001', r_user.user_id)
s_user.active_transaction.set_dollar_value(dollar_value)

print(f"un-timestamped Kafka Message {dollar_value} dollars")
print(s_user.active_transaction.generate_kafka_message())


print("current balances for both users")
print(f"sending user balance .... {s_user.return_balance()}")
print(f"receiving user balance .. {r_user.return_balance()}")

print('accepting details and sending...')

s_user.accept_transaction_details()
s_user.produce_message_to_kafka() 

print("sent - showing new balances")
print(f"sending user new balance .... {s_user.return_balance()}")
print(f"receiving user new balance .. {r_user.return_balance()}")
