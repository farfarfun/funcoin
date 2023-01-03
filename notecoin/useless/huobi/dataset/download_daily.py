import os
from datetime import datetime, timedelta

import pandas as pd
from notecoin.huobi.history.core import ALL_PERIODS, ALL_TYPES, load_daily_all
from notedrive.lanzou import LanZouCloud
from noteodps import opt
from odps import DataFrame
from tqdm import tqdm


def save_to_lanzou():
    downer = LanZouCloud()
    downer.ignore_limits()
    downer.login_by_cookie()
    start_date = datetime(2021, 5, 3)
    end_date = datetime(2021, 5, 31)
    duration = end_date - start_date
    for i in range(duration.days + 1):
        day = end_date - timedelta(days=i)
        file_path = load_daily_all(period='1min', date=day)
        downer.upload_file(file_path, folder_id='3359096')


def save_kline_to_odps(start_date=datetime(2021, 1, 1), end_date=datetime(2021, 5, 31)):
    t = opt.get_table("ods_notecoin_huobi_klines_data_d")

    def save_file_to_odps(file_path, partition):

        if t.exist_partition(partition):
            print(f'{partition} exist,drop it.')
            return
            t.delete_partition(partition_spec=partition)
        columns = ['symbol', 'id', 'open', 'close', 'low', 'high', 'vol', 'amount']
        dtype = {'symbol': 'str', 'id': 'long', 'open': 'float',
                 'close': 'float', 'low': 'float', 'high': 'float',
                 'vol': 'float', 'amount': 'float'}
        with t.open_writer(partition=partition, create_partition=True) as writer:
            for df in tqdm(
                    pd.read_csv(file_path, header=None, names=columns, dtype=dtype, chunksize=300000),
                    desc=os.path.basename(file_path)):
                writer.write(df.values.tolist())

    duration = end_date - start_date
    for period in ['1min']:  # ALL_PERIODS:
        for _type in ['future']:  # ALL_TYPES:
            for i in range(duration.days + 1):
                day = end_date - timedelta(days=i)
                partition = f"type='{_type}',period='{period}',ds='{day.strftime('%Y%m%d')}'"
                if t.exist_partition(partition):
                    continue

                file_path = load_daily_all(period=period, _type=_type, date=day)
                save_file_to_odps(file_path, partition)


def save_trade_to_odps(start_date=datetime(2021, 1, 1), end_date=datetime(2021, 5, 31)):
    t = opt.get_table("ods_notecoin_huobi_trades_data_d")

    def save_file_to_odps(file_path, partition):

        if t.exist_partition(partition):
            print(f'{partition} exist,drop it.')
            return
            t.delete_partition(partition_spec=partition)
        columns = ['symbol', 'tid', 'ts', 'price', 'amount', 'quantity', 'direction']
        dtype = {'symbol': 'str', 'tid': 'long', 'ts': 'long', 'price': 'float',
                 'amount': 'float', 'quantity': 'float', 'direction': 'str'}
        with t.open_writer(partition=partition, create_partition=True) as writer:
            for df in tqdm(
                    pd.read_csv(file_path, header=None, names=columns, dtype=dtype, chunksize=300000),
                    desc=os.path.basename(file_path)):
                writer.write(df.values.tolist())

    duration = end_date - start_date
    for period in ['1min']:  # ALL_PERIODS:
        for _type in ['future']:  # ALL_TYPES:
            for i in range(duration.days + 1):
                day = end_date - timedelta(days=i)
                partition = f"type='{_type}',period='{period}',ds='{day.strftime('%Y%m%d')}'"
                if t.exist_partition(partition):
                    continue

                file_path = load_daily_all(period=period, _type=_type, date=day, trade=True)
                save_file_to_odps(file_path, partition)


save_kline_to_odps(start_date=datetime(2021, 1, 1), end_date=datetime(2021, 10, 24))
save_trade_to_odps(start_date=datetime(2021, 1, 1), end_date=datetime(2021, 10, 24))
