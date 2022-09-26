from datetime import date, datetime, timedelta
import os
import sys
from typing import List
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd
from pandas_datareader import data
from tqdm import tqdm
import numpy as np

from cf_v2_src.rdb import (
    db,
    YFDailyStockpriceModel
)


INT_MAX = 2147483647-1
INT_MIN = -2147483648+1


def to_stockprice_obj(code, sr_stockprice):
    # if sr_stockprice.isnull().any():
    #     return None
    high = sr_stockprice[('High', f'{code}.T')]
    low = sr_stockprice[('Low', f'{code}.T')]
    open = sr_stockprice[('Open', f'{code}.T')]
    close = sr_stockprice[('Close', f'{code}.T')]
    volume = sr_stockprice[('Volume', f'{code}.T')]
    adj_close = sr_stockprice[('Adj Close', f'{code}.T')]
    if any([pd.isnull(d) for d in [high, low, open, close, volume, adj_close]]):
        return None
    date = sr_stockprice[('Date', '')]
    stockprice = YFDailyStockpriceModel(
        date=date,
        open=int(np.clip(round(open), INT_MIN, INT_MAX)),
        close=int(np.clip(round(close), INT_MIN, INT_MAX)),
        high=int(np.clip(round(high), INT_MIN, INT_MAX)),
        low=int(np.clip(round(low), INT_MIN, INT_MAX)),
        adj_close=int(np.clip(round(adj_close), INT_MIN, INT_MAX)),
        volume=int(np.clip(round(volume), INT_MIN, INT_MAX)),
        company_code=code,
    )
    return stockprice


def crawl(codes: List[int], start_dt: datetime, end_dt: datetime):
    target_codes = codes

    print(f'target codes : {target_codes}')

    sdt = datetime.now()
    df = data.DataReader([f"{code}.T" for code in target_codes], 'yahoo', start=start_dt, end=end_dt)
    edt = datetime.now()
    print(f'[stockprice crawl] took {(edt-sdt).total_seconds()}s')
    if len(df) == 0:
        print('len(df) == 0')
        return
    # print(df.reset_index())

    stockprices_to_insert_all = []

    sdt = datetime.now()
    for code in target_codes:
        stockprices = df.reset_index().apply(
            lambda x: to_stockprice_obj(code, x),
            axis=1,
        ).tolist()
        stockprices = [s for s in stockprices if s is not None]

        uniq_dates = [s.date for s in stockprices]

        alredy_exists_stockprices = db.query(
            YFDailyStockpriceModel
        ).filter(
            (YFDailyStockpriceModel.company_code == code) & (YFDailyStockpriceModel.date.in_(uniq_dates))
        ).all()

        alredy_exists_stockprice_dates = [s.date for s in alredy_exists_stockprices]

        stockprices_to_insert = [s for s in stockprices if s.date.date() not in alredy_exists_stockprice_dates]

        stockprices_to_insert_all += stockprices_to_insert

    edt = datetime.now()
    print(f'[select stockprice filter in uniq_dates] took {(edt-sdt).total_seconds()}s')

    print(f'len(stockprices_to_insert) : {len(stockprices_to_insert_all)}')

    if len(stockprices_to_insert_all) > 0:
        sdt = datetime.now()
        db.bulk_save_objects(stockprices_to_insert_all)
        db.commit()
        edt = datetime.now()
        print(f'[stockprice bulk insert] took {(edt-sdt).total_seconds()}s')



N_CODE_CUT = 100

df_stocklist = pd.read_csv(os.path.join(os.path.dirname(__file__), '..', 'cf_v2_src', 'stocklist_latest.csv'))
df_stocklist = df_stocklist[df_stocklist['市場・商品区分'].str.contains('内国株式')]

s_qcut, bins = pd.qcut(df_stocklist['コード'].unique(), N_CODE_CUT, labels=range(N_CODE_CUT), retbins=True)
# print(bins)
df_cut = [df_stocklist[(df_stocklist['コード'] >= int(s_code)) & (df_stocklist['コード'] <= int(e_code))] for s_code, e_code in zip(bins[:-1], bins[1:])]
print([len(df) for df in df_cut])

for idx in tqdm(range(N_CODE_CUT)):
    if idx < 5:
        print(f'skipping idx {idx}')
        continue
    target_codes = df_cut[idx]['コード'].sort_values().tolist()

    crawl(target_codes, date.today() - timedelta(days=365*5), date.today())
