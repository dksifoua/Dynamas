import sys
import argparse
import pandas as pd

from src.data_extraction.hbase_api import HBaseRestAPI
from src.data_extraction.types import Action, FilterType, Operator, Comparator, NbCol, Category
from src.data_extraction.utils import extract_json, get_batch, build_xml, build_filter, to_datetime, to_timestamp, encode, decode

ap = argparse.ArgumentParser()
ap.add_argument("-a", "--address", required=True, help="HBase server address")
ap.add_argument("-p", "--port", required=True, help="HBase server port")
ap.add_argument("-s", "--symbols", required=True, help="Symbols")
args = vars(ap.parse_args())

if __name__ == "__main__":
    symbols = args['symbols'].split(',')
    nb_rows = 10
    start = to_timestamp('2019-07-09 01:00:00.000000-04:00')
    end = to_timestamp('2019-07-09 20:00:00.000000-04:00')

    # ===================================================================================================
    # HBase database connection
    # ===================================================================================================
    api = HBaseRestAPI(address=args['address'], port=args['port'])
    api.connect()

    # ===================================================================================================
    # Stock data
    # ===================================================================================================
    # table = 'stock'
    # for symbol in symbols:
    #     xml = build_xml(
    #         batch=get_batch(nb_rows, NbCol.STOCK),
    #         filters=[
    #             build_filter(
    #                 filter_type=FilterType.ROW_FILTER,
    #                 operator=Operator.EQUAL,
    #                 comparator_type=Comparator.BINARY_COMPARATOR,
    #                 comparator_value=Action.__dict__[symbol],
    #             )
    #         ],
    #         start_time=start,
    #         end_time=end
    #     )
    #     endpoint = api.put_table_scanner(table_name=table, xml=xml)
    #     data_batches = api.get_table_content(endpoint)
        
    #     dfs = []
    #     while True:
    #         try:
    #             json_data = data_batches.__next__()
    #             df = extract_json(json_data)
    #             dfs.append(df)
    #         except:
    #             break

    #     if len(dfs) > 0:
    #         dfs = pd.concat(dfs, ignore_index=True)

    #         filename = f'data/{table}_{symbol}.csv'
    #         dfs.to_csv(filename, index=False)
    #         print(f'Retrieving {table} data for {symbol} -> ok!')
    #     else:
    #         print(f'No data in {table} table for {symbol}')

    # ===================================================================================================
    # Rawtweets data (action's type)
    # ===================================================================================================
    table = 'rawtweets'
    for symbol in symbols:
        xml = build_xml(
            batch=get_batch(nb_rows, NbCol.RAWTWEETS),
            filters=[
                build_filter(
                    filter_type=FilterType.SINGLE_COLUMN_VALUE_FILTER,
                    operator=Operator.EQUAL,
                    comparator_type=Comparator.BINARY_COMPARATOR,
                    comparator_value=Action.__dict__[symbol],
                    family=encode('tweetsData'.encode('utf-8')),
                    qualifier=encode('symbols'.encode('utf-8'))
                )
            ],
            # start_time=start,
            # end_time=end
        )
        endpoint = api.put_table_scanner(table_name=table, xml=xml)
        data_batches = api.get_table_content(endpoint)
        
        dfs = []
        while True:
            try:
                json_data = data_batches.__next__()
                df = extract_json(json_data)
                dfs.append(df)
            except:
                break

        if len(dfs) > 0:
            dfs = pd.concat(dfs, ignore_index=True)

            filename = f'data/{table}_{symbol}.csv'
            dfs.to_csv(filename, index=False)
            print(f'Retrieving {table} data for {symbol} -> ok!')
        else:
            print(f'No data in {table} table for {symbol}')

    # ===================================================================================================
    # Rawtweets data (other categories: financial & trend)
    # ===================================================================================================
    # table = 'stock'
    # for category in ['TREND', 'FINANCIAL']:
    #     xml = build_xml(
    #         batch=get_batch(nb_rows, NbCol.RAWTWEETS),
    #         filters=[
    #             build_filter(
    #                 filter_type=FilterType.SINGLE_COLUMN_VALUE_FILTER,
    #                 operator=Operator.EQUAL,
    #                 comparator_type=Comparator.BINARY_COMPARATOR,
    #                 comparator_value=Category.__dict__[category],
    #                 family=encode('tweetsData'.encode('utf-8')),
    #                 qualifier=encode('category'.encode('utf-8'))
    #             )
    #         ]
    #     )
    #     endpoint = api.put_table_scanner(table_name=table, xml=xml)
    #     data_batches = api.get_table_content(endpoint)
        
    #     dfs = []
    #     while True:
    #         try:
    #             json_data = data_batches.__next__()
    #             df = extract_json(json_data)
    #             dfs.append(df)
    #         except:
    #             break

    #     if len(dfs) > 0:
    #         dfs = pd.concat(dfs, ignore_index=True)

    #         filename = f'data/{table}_{symbol}.csv'
    #         dfs.to_csv(filename, index=False)
    #         print(f'Retrieving {symbol} {table} data for -> ok!')
    #     else:
    #         print(f'No data in {table} {symbol} table for')
