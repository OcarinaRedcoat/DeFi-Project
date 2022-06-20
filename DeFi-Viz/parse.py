
import csv
from collections import defaultdict
import json
import sys
from datetime import datetime
from tqdm import tqdm
from numpy import double
import pandas as pd
import os

from datetime import datetime
from algosdk.v2client import indexer
from algosdk.error import IndexerHTTPError



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

    wallet_cache = dict()


    links = []
    nodes = []


    file_name = sys.argv[1]
    print("File name .csv :" + str(file_name))

    with open(file_name, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        
        for row in tqdm(reader):
# XFYAYSEGQIY2J3DCGGXCPXY5FGHSVKM3V4WCNYCLKDLHB7RYDBU233QB5M

            sender = wallet_cache.get(row['sender'])
            receiver = wallet_cache.get(row['receiver'])

            if (sender == None): 
                sender = Wallet(row['sender'], 0)
                node = { "id": sender._address, "group" : 1 }
                nodes.append(node)
                wallet_cache[row['sender']] = sender

            if (receiver == None):
                receiver = Wallet(row['receiver'], 0)
                node = { "id": receiver._address, "group" : 1 }
                nodes.append(node)
                index = len(wallet_cache)
                wallet_cache[index] = {receiver._address : "1"}
                wallet_cache[row['receiver']] = receiver

            link = { "source" : sender._address, "target" : receiver._address, "value" : 1, "timestamp" : str(datetime.fromtimestamp(int(row["timestamp"]))) ,"amount" :row['amount'], "id" : row['id']}

            links.append(link)

    
    json_dict = { "nodes" : nodes, "links" : links}

    json_object = json.dumps(json_dict, indent = 4) 

    f = open("data/"+ sys.argv[2] + ".json", "w")
    f.write(json_object)
    f.close()
