import os
import sys
from typing import Dict
import pandas as pd
from algosdk.v2client import indexer
from algosdk.error import IndexerHTTPError
from pkg_resources import require
from tqdm import tqdm
from time import time

from idx_address import *

TIME_LIMIT_SECONDS = 3600
HALT_LEEWAY_SECONDS = 120
USE_INTERNAL_INDEXER = False


def extract_row_data(response_row: Dict) -> Dict:
    row_data = {}
    
    row_data["id"] = response_row["id"]
    row_data["sender"] = response_row["sender"]
    row_data["tx_type"] = response_row["tx-type"]
    
    if row_data["tx_type"] == "pay":
        row_data["amount"] = response_row["payment-transaction"]["amount"]
        row_data["receiver"] = response_row["payment-transaction"]["receiver"]

    return row_data


if __name__ == "__main__":

    start_time = time()
    HALT_THRESHOLD = TIME_LIMIT_SECONDS - HALT_LEEWAY_SECONDS

    min_round = 18150150 

    print("Determined min_round = {}".format(min_round))

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

    row_list = []
    current_round = min_round + 10
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
                    
                    row_list.append(extract_row_data(response_row))

        time_now = time() - start_time
        if (not QUERY_SUCCESS) or (time_now > HALT_THRESHOLD):
            max_round_inclusive = request_round
            current_round = max_round_inclusive + 1
            print(
                "Halted download after round {}!".format(max_round_inclusive))
            break

    print("Completed parsing, creating DataFrame...")
    df = pd.DataFrame(row_list,
                      columns=[
                          "id", "sender",
                          "receiver",
                          "amount", "tx_type"
                      ])

    print("Created DataFrame, now saving...")
    tempdir = "extract-data"
    os.makedirs(tempdir, exist_ok=True)
    filename = "{}_{}.csv".format(
        str(min_round).rjust(9, "0"),
        str(current_round).rjust(9, "0"))
    temppath = os.path.join(tempdir, filename)
    df.to_csv(temppath, index=False)

    print("Done! Time now: {} seconds.".format(time() - start_time))
