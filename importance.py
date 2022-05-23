from fileinput import filename
import networkx as nx
import csv
import json
import sys
from tqdm import tqdm
from time import time
import operator


if __name__ == "__main__":

    print("Starting...")
    start_time = time()
    #sys.setrecursionlimit(10**6)
    wallet_map = dict()
    wallet_map_inverse = dict()
    wallet_list = list()

    tx_list = list()

    file_name = sys.argv[1]

    print("Parsing csv file: ", file_name)


    G = nx.DiGraph()




    with open(file_name, newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        for row in tqdm(reader):
            if wallet_map.get(row['sender']) == None:
                wallet_list.append(row['sender'])
                G.add_node(len(wallet_list)-1, wallet=row['sender'])
                
                index = len(wallet_list)-1
                wallet_map[row['sender']] = index 
                wallet_map_inverse[index] = row['sender']

            if wallet_map.get(row['receiver']) == None:
                wallet_list.append(row['receiver'])
                G.add_node(len(wallet_list)-1, wallet=row['receiver'])

                index = len(wallet_list)-1
                wallet_map[row['receiver']] = index
                wallet_map_inverse[index] = row['receiver']

            tx_list.append((wallet_map[row['sender']], wallet_map[row['receiver']], row['amount']))

    print("Creating nx.DiGraph and adding edges...")

    for e in tqdm(tx_list):
        G.add_edge(e[0], e[1], weight=e[2])

    print("Calculating pagerank...")


    pr = nx.pagerank(G)

    print("Calculating degree centrality...")

    dc = nx.degree_centrality(G)

    dict(sorted(dc.items(), key=lambda item: item[1], reverse=True))


    final_pr = dict()
    final_dc = dict()
    for key in pr:
        final_pr[wallet_map_inverse[key]] = pr[key]
    for key in dc:
        final_dc[wallet_map_inverse[key]] = dc[key]

    pr_sortdict = dict( sorted(final_pr.items(), key=operator.itemgetter(1),reverse=True))
    dc_sortdict = dict( sorted(final_dc.items(), key=operator.itemgetter(1),reverse=True))


    print("Writing to Files...")

    filename_split = file_name.split('/')

    folder_name = filename_split[0]

    print(filename_split)

    extract_name = filename_split[1].split('.')

    pr_file = open(filename_split[0] + '/output/' + extract_name[0] + "_pagerank.csv", "w")


    writer = csv.writer(pr_file)
    writer.writerow(["wallet", "pr"])
    for key, value in pr_sortdict.items():
        writer.writerow([key, value])


    dc_file = open(filename_split[0] + '/output/' + extract_name[0] + "_degree_centrality.csv", "w")

    writer = csv.writer(dc_file)

    writer.writerow(["wallet", "dc"])
    for key, value in dc_sortdict.items():
        writer.writerow([key, value])

    print("Network of ", len(wallet_list), " nodes (wallets) and ", len(tx_list), " edges (transactions)")

    print("Done! Time now: {} seconds.".format(time() - start_time))