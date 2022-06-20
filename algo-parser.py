import csv
from collections import defaultdict
import json
import sys
from datetime import datetime
from tqdm import tqdm
from numpy import double
import pandas as pd
import os

from algosdk.v2client import indexer
from algosdk.error import IndexerHTTPError

from idx_address import *

USE_INTERNAL_INDEXER = False
class Transaction:
    def __init__(self, id, sender, receiver, amount, timestamp):
        self._id = id

        if (sender != None):
            self._sender = sender
        else:
            self._sender = None

        if (receiver != None):
            self._receiver = receiver
        else:
            self._receiver = None

        self._amount = amount
        self._timestamp = timestamp
        self._dateTime = str(datetime.fromtimestamp(timestamp))

    def toString(self):
        if (self._sender != None):
            return str({self._id : {"sender" : self._sender, "amount" : self._amount, "timestamp" : self._timestamp, "date time" : self._dateTime}})
        elif (self._receiver != None):
            return str({self._id : {"receiver" : self._receiver, "amount" : self._amount, "timestamp" : self._timestamp, "date time" : self._dateTime}})
        
class Wallet:
    def __init__(self, address, amount):
        self._address = address
        self._amount = amount
        self._transactions = list()
        self._transactionsString = list()
        

    def txToString(self, tx):
        self._transactionsString.append(tx)

    def senderTx(self, tx):
        self._transactions.append(tx)
        self._amount -= tx._amount
        self.txToString(tx.toString())

    def receiverTx(self, tx):
        self._transactions.append(tx)
        self._amount += tx._amount
        self.txToString(tx.toString())

    def getTx(self):
        return self._transactions


if __name__ == "__main__":

    if USE_INTERNAL_INDEXER:
        url = os.environ.get("AF_IDX_ADDRESS")
        assert url, "Environment variable AF_IDX_ADDRESS not available!"
    else:
        url = ALGOEXP_IDX_ADDRESS
    indexer_client = indexer.IndexerClient("",
                                           url,
                                           headers={"User-Agent": "Web App"})
    print("Initialized indexer...")


    wallet_dict = dict()
    wallet_cache = dict()
    row_csv = []


    file_name = sys.argv[1]
    print("File name .csv :" + str(file_name))

    flagJSON = input("Create Json Parsed [y/n]: ")

    flagCSV = input("Create CSV Parsed [y/n]: ")

    with open(file_name, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        
        for row in tqdm(reader):

            if (double(row['amount'])*(pow(10, -6)) == 0.0):
                continue
            
            if flagJSON == 'y':

                sender = wallet_cache.get(row['sender'])
                receiver = wallet_cache.get(row['receiver'])

                if (sender == None): 
                    #response = indexer_client.account_info(row['sender'], block=18363444)
                    
                    #print(response , '\n' , response['account']['amount'] ,  '\n')
                    sender = Wallet(row['sender'], 0)

                    #FIXME: Query ammount of money of the sender
                if (receiver == None):
                    #response = indexer_client.account_info(row['receiver'], block=18363444)
                    #print(response)
                    receiver = Wallet(row['receiver'], 0)
                    #FIXME: Query ammount of money of the receiver

                senderTx = Transaction(row['id'], None, row['receiver'], double(row['amount'])*(pow(10, -6)), int(row['timestamp']))
                receiverTx = Transaction(row['id'], row['sender'], None, double(row['amount'])*(pow(10, -6)), int(row['timestamp']))


                sender.senderTx(senderTx)
                receiver.receiverTx(receiverTx)

                wallet_cache[row['sender']] = sender
                wallet_cache[row['receiver']] = receiver

                wallet_dict.update({sender._address : {"amount": sender._amount, "numberOfTX": len(sender._transactions), "transactions" : sender._transactionsString}})
                wallet_dict.update({receiver._address : {"amount": receiver._amount, "numberOfTX": len(receiver._transactions), "transactions" : receiver._transactionsString}})
        
            if flagCSV == 'y':

                row_data = {}
                row_data["id"] = row["id"]
                row_data["sender"] = row["sender"]
                row_data["timestamp"] = row['timestamp']
                row_data["amount"] = double(row['amount'])*(pow(10, -6))
                row_data["receiver"] = row["receiver"]

                row_csv.append(row_data)

    
    
    if flagJSON == 'y':
        json_object = json.dumps(wallet_dict, indent = 4) 

        f = open(file_name + ".json", "w")
        f.write(json_object)
        f.close()


    if flagCSV == 'y':
        df = pd.DataFrame(row_csv,
                      columns=[
                          "id", "sender",
                          "receiver",
                          "amount", "tx_type", "timestamp"
                      ])
        print("Created DataFrame, now saving...")

        df.to_csv(file_name + "_parsed.csv", index=False)