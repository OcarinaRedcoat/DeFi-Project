import csv
from collections import defaultdict
import json
import sys
from datetime import datetime

from numpy import double

class Transaction:
    def __init__(self, id, receiver, amount, timestamp):
        self._id = id
        self._receiver = receiver
        self._amount = amount
        self._timestamp = timestamp
        self._dateTime = str(datetime.fromtimestamp(timestamp))
        
class Wallet:
    def __init__(self, address, amount):
        self._address = address
        self._amount = amount
        self._transactions = list()

    def senderTx(self, tx):
        self._amount -= tx._amount

    def receiverTx(self, tx):
        self._transactions.append(tx)
        self._amount += tx._amount

    def getTx(self):
        return self._transactions

wallet_dict = dict()
wallet_cache = dict()

file_name = sys.argv[1]
print("File name .csv :" + str(file_name))

with open(file_name, newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        sender = wallet_cache.get(row['sender'])
        receiver = wallet_cache.get(row['receiver'])

        if (sender == None):
            sender = Wallet(row['sender'], 0)
        if (receiver == None):
            receiver = Wallet(row['receiver'], 0)
            
        tx = Transaction(row['id'], row['receiver'], double(row['amount'])*(pow(10, -6)), int(row['timestamp']))
        sender.senderTx(tx)
        receiver.receiverTx(tx)

        wallet_cache[row['sender']] = sender
        wallet_cache[row['receiver']] = receiver

        senderListOfTx = sender.getTx()
        receiverListOfTx = receiver.getTx()
        senderTx = dict()
        receiverTx = dict()

        for newTx in senderListOfTx:
            senderTx.update({newTx._id : {"receiver" : newTx._receiver, "amount" : newTx._amount, "timestamp" : newTx._timestamp, "date time" : newTx._dateTime}})

        for newTx in receiverListOfTx:
            receiverTx.update({newTx._id : {"receiver" : newTx._receiver, "amount" : newTx._amount, "timestamp" : newTx._timestamp, "date time" : newTx._dateTime}})
        
        wallet_dict.update({sender._address : {"amount": sender._amount, "transactions" : senderTx}})
        wallet_dict.update({receiver._address : {"amount": receiver._amount, "transactions" : receiverTx}})
    


json_object = json.dumps(wallet_dict, indent = 4) 

f = open(file_name + ".json", "w")
f.write(json_object)
f.close()