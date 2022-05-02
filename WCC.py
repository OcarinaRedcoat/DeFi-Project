# Python program to print connected
# components in an undirected graph
 
 
import csv
import json
import sys
from tqdm import tqdm
from time import time


class Graph:
 

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
 

if __name__ == "__main__":

    print("Starting...")
    start_time = time()
    sys.setrecursionlimit(10**6)
    wallet_map = dict()
    wallet_map_inverse = dict()
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
                wallet_map_inverse[index] = row['sender']

            if wallet_map.get(row['receiver']) == None:
                wallet_list.append(row['receiver'])
                index = len(wallet_list)-1
                wallet_map[row['receiver']] = index
                wallet_map_inverse[index] = row['receiver']

            tx_list.append((wallet_map[row['sender']], wallet_map[row['receiver']]))

    print("Creating graph and adding edges...")
    g = Graph(len(wallet_list))
    for e in tqdm(tx_list):
        g.addEdge(e[0], e[1])

    print("Calculating number of WCCs...")

    cc, largest_cc = g.connectedComponents()
    largest_wcc_mapped = list()
    print("Length of the Biggest WCC: ", len(largest_cc))
    for i in tqdm(range(len(largest_cc))):
        wallet_map_id = largest_cc[i]
        largest_wcc_mapped.append(wallet_map_inverse.get(wallet_map_id))

    with open(file_name + "_largest_wcc", "w") as f:
        f.write(json.dumps(largest_wcc_mapped))
        f.close()

    print("Network of ", len(wallet_list), " nodes (wallets) and ", len(tx_list), " edges (transactions)")
    print("Number of WCCs", len(cc))

    print("Done! Time now: {} seconds.".format(time() - start_time))