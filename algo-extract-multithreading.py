import os
import sys
from typing import Dict
import pandas as pd
from algosdk.v2client import indexer
from algosdk.error import IndexerHTTPError
from pkg_resources import require
from tqdm import tqdm
from time import time

import threading


from idx_address import *

USE_INTERNAL_INDEXER = False

row_list_per_process = [[], [], [], []]


def extract_row_data(response_row: Dict, timestamp) -> Dict:
    row_data = {}
    
    row_data["id"] = response_row["id"]
    row_data["sender"] = response_row["sender"]
    row_data["tx_type"] = response_row["tx-type"]

    row_data["timestamp"] = timestamp

    if row_data["tx_type"] == "pay":
        row_data["amount"] = response_row["payment-transaction"]["amount"]
        row_data["receiver"] = response_row["payment-transaction"]["receiver"]

    return row_data

def main(min_round, current_round, indexer_client, index):


    for request_round in tqdm(range(min_round, current_round)):

        try:
            indexer_response = indexer_client.block_info(block=request_round)
            QUERY_SUCCESS = True
        except IndexerHTTPError:
            print("Error occurred at block {}, terminating...".format(
                request_round))
            QUERY_SUCCESS = False
            request_round -= 1

        if QUERY_SUCCESS:
            for response_row in indexer_response["transactions"]:

                if(response_row['tx-type'] == 'pay'):
                    
                    row_list_per_process[index].append(extract_row_data(response_row, indexer_response['timestamp']))

        if (not QUERY_SUCCESS):
            max_round_inclusive = request_round
            current_round = max_round_inclusive + 1
            print(
                "Halted download after round {}!".format(max_round_inclusive))
            break


if __name__ == "__main__":

    start_time = time()

    # TODO: Refazer of 10000 primeiros
    min_round = 18363444 + 10000 + 10000 # FIRST BLOCK OF 2022 

    round_per_thread = int(int(sys.argv[1])/4)

    min_round_thead1 = 18363444 + 10000 # FIRST BLOCK OF 2022 
    min_round_thead2 = 18363444 + 10000 + round_per_thread # FIRST BLOCK OF 2022 
    min_round_thead3 = 18363444 + 10000 + (round_per_thread*2) # FIRST BLOCK OF 2022 
    min_round_thead4 = 18363444 + 10000 + (round_per_thread*3) # FIRST BLOCK OF 2022 

    print("Determined min_round = {}".format(min_round))
    print("Determined round per thread = {}".format(round_per_thread))

    if USE_INTERNAL_INDEXER:
        url = os.environ.get("AF_IDX_ADDRESS")
        assert url, "Environment variable AF_IDX_ADDRESS not available!"
    else:
        url = ALGOEXP_IDX_ADDRESS
    indexer_client = indexer.IndexerClient("",
                                           url,
                                           headers={"User-Agent": "Web App"})
    print("Initialized indexer...")

    current_round = indexer_client.search_transactions()["current-round"]
    print("Found current round is {}".format(current_round))
    max_round_inclusive = current_round - 1


    current_round_thread1 = min_round_thead1 + round_per_thread
    current_round_thread2 = min_round_thead2 + round_per_thread
    current_round_thread3 = min_round_thead3 + round_per_thread
    current_round_thread4 = min_round_thead4 + round_per_thread

    t1 = threading.Thread(target=main, args=(min_round_thead1, current_round_thread1, indexer_client, 0,))
    t2 = threading.Thread(target=main, args=(min_round_thead2, current_round_thread2, indexer_client, 1,))
    t3 = threading.Thread(target=main, args=(min_round_thead3, current_round_thread3, indexer_client, 2,))
    t4 = threading.Thread(target=main, args=(min_round_thead4, current_round_thread4, indexer_client, 3,))

    t1.start()
    t2.start()
    t3.start()
    t4.start()

    t1.join()
    t2.join()
    t3.join()
    t4.join()

    row_list = row_list_per_process[0] + row_list_per_process[1]+ row_list_per_process[2] + row_list_per_process[3]

    print("Completed parsing, creating DataFrame...")

    df = pd.DataFrame(row_list,
                      columns=[
                          "id", "sender",
                          "receiver",
                          "amount", "tx_type", "timestamp"
                      ])

    print("Created DataFrame, now saving...")
    tempdir = "extract-data"
    os.makedirs(tempdir, exist_ok=True)
    filename = "AAAAAAA_{}_{}.csv".format(
        str(min_round).rjust(9, "0"),
        str(current_round).rjust(9, "0"))
    temppath = os.path.join(tempdir, filename)
    df.to_csv(temppath, index=False)


    print("Done! Time now: {} seconds.".format(time() - start_time))
