import csv
from datetime import datetime
import json
import sys
from collections import defaultdict
from tqdm import tqdm
from time import time


class UndirectedGraph:

    def __init__(self, V):
        self.V = V
        self.adj = [[] for i in range(V)]
 
    def DFSUtil(self, temp, v, visited):
 
        visited[v] = True
 
        temp.append(v)
 
        for i in self.adj[v]:
            if visited[i] == False:
 
                temp = self.DFSUtil(temp, i, visited)
        return temp
 
    def addEdge(self, v, w):
        self.adj[v].append(w)
        self.adj[w].append(v)
 
    def connectedComponents(self):
        visited = []
        cc = []
        largest_wcc = []
        for i in range(self.V):
            visited.append(False)
        for v in range(self.V):
            if visited[v] == False:
                temp = []
                self.DFSUtil(temp, v, visited)
                cc.append(self.DFSUtil(temp, v, visited))
            if len(largest_wcc) < len(temp):
                largest_wcc.clear()
                largest_wcc = temp.copy()
        return cc, largest_wcc

class DirectedGraph:
  
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

        scc = list()
        w = -1
        if low[u] == disc[u]:
            self.numberSCC += 1
            while w != u:
                w = st.pop()
                scc.append(w)
                stackMember[w] = False
        return scc
                 
    def SCC(self):
  
        disc = [-1] * (self.V)
        low = [-1] * (self.V)
        stackMember = [False] * (self.V)
        st =[]
        bscc = list()
        for i in range(self.V):
            if disc[i] == -1:
                scc = self.SCCUtil(i, low, disc, stackMember, st)
            if len(bscc) < len(scc):
                bscc.clear()
                bscc = scc.copy()
        return bscc
 
  
  

if __name__ == "__main__":

    print("Starting...")
    start_time = time()
    sys.setrecursionlimit(10**6)


    file_name = sys.argv[1]


    evalutation_wallet = dict()
    evalutation_tx_list = dict()
    wallet_map = dict()
    wallet_map_inverse = dict()

    for i in range(12):
        wallet_map[i +1 ] = dict()
        wallet_map_inverse[i +1 ] = dict()
        evalutation_tx_list[i +1 ] = list()
        evalutation_wallet[i + 1] = list()

    print("Parsing csv file: ", file_name)

    with open(file_name, newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        current_month = 1
        for row in tqdm(reader):

            eval_month = datetime.fromtimestamp(int(row['timestamp']))

            if eval_month.month > current_month:
                evalutation_wallet[eval_month.month] = evalutation_wallet[current_month].copy()
                evalutation_tx_list[eval_month.month] = evalutation_tx_list[current_month].copy()
                wallet_map[eval_month.month] = wallet_map[current_month].copy()
                wallet_map_inverse[eval_month.month] = wallet_map_inverse[current_month].copy()
                current_month = eval_month.month

            if wallet_map[current_month].get(row['sender']) == None:
                evalutation_wallet[current_month].append(row['sender'])
                index = len(evalutation_wallet[current_month])-1
                wallet_map[current_month][row['sender']] = index 
                wallet_map_inverse[current_month][index] = row['sender']

            if wallet_map[current_month].get(row['receiver']) == None:
                evalutation_wallet[current_month].append(row['receiver'])
                index = len(evalutation_wallet[current_month])-1
                wallet_map[current_month][row['receiver']] = index
                wallet_map_inverse[current_month][index] = row['receiver']

            evalutation_tx_list[current_month].append((wallet_map[current_month][row['sender']], wallet_map[current_month][row['receiver']]))

    print("Creating graph and adding edges...")

    
    undirected_g = list()
    directed_g = list()
    for i in range(12):
        if len(evalutation_wallet[i+1]) != 0:
            undirected_g.append(UndirectedGraph(len(evalutation_wallet[i+1])))
            directed_g.append(DirectedGraph(len(evalutation_wallet[i+1])))
            for e in tqdm(evalutation_tx_list[i+1]):
                #print(len(evalutation_tx_list[i+1]))
                undirected_g[i].addEdge(e[0], e[1])
                directed_g[i].addEdge(e[0], e[1])

    print("Calculating graph's number of SCC and WCC...")


    metrics = list()


    for i in range(len(directed_g)):

        cc, largest_cc = undirected_g[i].connectedComponents()
        largest_scc = directed_g[i].SCC()

        monthly_metrics = list()
        datetime_object = datetime.strptime(str(i+1), "%m")
        month_name = datetime_object.strftime("%B")

        monthly_metrics.append(month_name)
        monthly_metrics.append(str(len(cc)))
        monthly_metrics.append(str(directed_g[i].numberSCC))
        monthly_metrics.append(str(directed_g[i].V))
        monthly_metrics.append(str(len(evalutation_tx_list[i+1])))
        monthly_metrics.append(str(len(largest_scc)))
        monthly_metrics.append(str(len(largest_cc)))

        metrics.append(monthly_metrics)
        print("Number of WCCs: ", len(cc), " Month: " , i+1)
        print("Number of SCCs: ", directed_g[i].numberSCC, " Month: " , i+1)
        print("Number of nodes: ", directed_g[i].V)
        print("Number of transactions: ", len(evalutation_tx_list[i+1]))
        print("Size of the largest SCC: ", len(largest_scc))
        print("Size of the largest WCC: ", len(largest_cc))


    header = ['Month', 'nWCC', 'nSCC', 'nNodes', 'nTx', 'biggestSCC', 'biggestWCC']
    with open('extract-data/output/metrics.csv', 'w', encoding='UTF8', newline='') as f:
        writer = csv.writer(f)

        # write the header
        writer.writerow(header)
        # write multiple rows
        writer.writerows(metrics)

    print("Done! Time now: {} seconds.".format(time() - start_time))
