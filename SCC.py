import csv
from collections import defaultdict
from fileinput import filename
import json
import sys
from datetime import datetime
from tqdm import tqdm
from numpy import double
from time import time

from collections import defaultdict
  
class Graph:
  
    def __init__(self,vertices):
        self.V= vertices
        
        self.graph = defaultdict(list)
         
        self.Time = 0

        self.numberSCC = 0
  
    def addEdge(self,u,v):
        self.graph[u].append(v)
         

    def SCCUtil(self,u, low, disc, stackMember, st):
 
        disc[u] = self.Time
        low[u] = self.Time
        self.Time += 1
        stackMember[u] = True
        st.append(u)
 
        for v in self.graph[u]:
             
            if disc[v] == -1 :
             
                self.SCCUtil(v, low, disc, stackMember, st)

                low[u] = min(low[u], low[v])
                         
            elif stackMember[v] == True:
 
                low[u] = min(low[u], disc[v])
 
        w = -1
        if low[u] == disc[u]:
            self.numberSCC += 1
            while w != u:
                w = st.pop()

                stackMember[w] = False
                 
    def SCC(self):
  
        disc = [-1] * (self.V)
        low = [-1] * (self.V)
        stackMember = [False] * (self.V)
        st =[]
         
        for i in tqdm(range(self.V)):
            if disc[i] == -1:
                self.SCCUtil(i, low, disc, stackMember, st)
 
 
  
  
  

if __name__ == "__main__":

    print("Starting...")
    start_time = time()
    sys.setrecursionlimit(10**6)
    wallet_map = dict()
    wallet_list = list()

    tx_list = list()

    file_name = sys.argv[1]

    print("Parsing csv file: ", file_name)

    with open(file_name, newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        for row in tqdm(reader):
            if wallet_map.get(row['sender']) == None:
                wallet_list.append(row['sender'])
                index = len(wallet_list)-1
                wallet_map[row['sender']] = index 

            if wallet_map.get(row['receiver']) == None:
                wallet_list.append(row['receiver'])
                index = len(wallet_list)-1
                wallet_map[row['receiver']] = index

            tx_list.append((wallet_map[row['sender']], wallet_map[row['receiver']]))

    print("Creating graph and adding edges...")
    g = Graph(len(wallet_list))
    for e in tqdm(tx_list):
        g.addEdge(e[0], e[1])

    print("Calculating number of SCCs...")

    g.SCC()

    print("Network of ", len(wallet_list), " nodes (wallets) and ", len(tx_list), " edges (transactions)")
    print("Number of SCCs", g.numberSCC)

    print("Done! Time now: {} seconds.".format(time() - start_time))