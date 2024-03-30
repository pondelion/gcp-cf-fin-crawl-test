from datetime import date, datetime, timedelta
import os

import pandas as pd
import numpy as np
from pandas_datareader import data

from rdb import (
    db,
    YFDailyStockpriceModel,
    StooqDailyStockpriceModel,
)


def to_yf_stockprice_obj(code, sr_stockprice):
    if sr_stockprice.isnull().any():
        return None
    high = round(sr_stockprice[('High', f'{code}.T')])
    low = round(sr_stockprice[('Low', f'{code}.T')])
    open = round(sr_stockprice[('Open', f'{code}.T')])
    close = round(sr_stockprice[('Close', f'{code}.T')])
    volume = round(sr_stockprice[('Volume', f'{code}.T')])
    adj_close = round(sr_stockprice[('Adj Close', f'{code}.T')])
    date = sr_stockprice[('Date', '')]
    stockprice = YFDailyStockpriceModel(
        date=date,
        open=open,
        close=close,
        high=high,
        low=low,
        adj_close=adj_close,
        volume=volume,
        company_code=code,
    )
    return stockprice


def to_stooq_stockprice_obj(code, sr_stockprice):
    # if sr_stockprice.isnull().any():
    #     return None
    high = sr_stockprice[('High', f'{code}.JP')]
    low = sr_stockprice[('Low', f'{code}.JP')]
    open = sr_stockprice[('Open', f'{code}.JP')]
    close = sr_stockprice[('Close', f'{code}.JP')]
    volume = sr_stockprice[('Volume', f'{code}.JP')]
    if any([pd.isnull(d) for d in [high, low, open, close, volume]]):
        return None
    date = sr_stockprice[('Date', '')]
    stockprice = StooqDailyStockpriceModel(
        date=date,
        open=open,
        close=close,
        high=high,
        low=low,
        volume=volume,
        company_code=code,
    )
    return stockprice


def crawl_yf(code_cut_idx: int, n_code_cut: int = int(os.environ.get('N_CODE_CUT', 100))):
    df_stocklist = pd.read_csv('./stocklist_latest.csv')
    df_stocklist = df_stocklist.replace('-', np.nan)
    # TARGET_MARKETS = ['プライム（内国株式）', 'スタンダード（内国株式）', 'グロース（内国株式）']
    df_stocklist = df_stocklist[df_stocklist['市場・商品区分'].str.contains('内国株式')]

    s_qcut, bins = pd.qcut(df_stocklist['コード'].unique(), n_code_cut, labels=range(n_code_cut), retbins=True)
    print(bins)
    df_cut = [df_stocklist[(df_stocklist['コード'] >= int(s_code)) & (df_stocklist['コード'] <= int(e_code))] for s_code, e_code in zip(bins[:-1], bins[1:])]
    print([len(df) for df in df_cut])

    target_codes = df_cut[code_cut_idx]['コード'].tolist()

    df = data.DataReader([f"{code}.T" for code in target_codes], 'yahoo', start=date.today()-timedelta(days=4), end=date.today())
    if len(df) == 0:
        print(f'len(df) == 0 : {df.index.name}')
        return
    print(df.reset_index())

    stockprices_to_insert_all = []

    for code in target_codes:
        stockprices = df.reset_index().apply(
            lambda x: to_yf_stockprice_obj(code, x),
            axis=1,
        ).tolist()
        stockprices = [s for s in stockprices if s is not None]

        uniq_dates = [s.date for s in stockprices]
        sdt = datetime.now()
        alredy_exists_stockprices = db.query(
            YFDailyStockpriceModel
        ).filter(
            (YFDailyStockpriceModel.company_code == code) & (YFDailyStockpriceModel.date.in_(uniq_dates))
        ).all()
        edt = datetime.now()
        print(f'[select stockprice {code} filter in uniq_dates] took {(edt-sdt).total_seconds()}s')
        alredy_exists_stockprice_dates = [s.date for s in alredy_exists_stockprices]

        stockprices_to_insert = [s for s in stockprices if s.date.date() not in alredy_exists_stockprice_dates]

        stockprices_to_insert_all += stockprices_to_insert

    print(f'len(stockprices_to_insert) : {len(stockprices_to_insert_all)}')

    if len(stockprices_to_insert_all) > 0:
        sdt = datetime.now()
        db.bulk_save_objects(stockprices_to_insert_all)
        db.commit()
        edt = datetime.now()
        print(f'[stockprice bulk insert] took {(edt-sdt).total_seconds()}s')


def crawl_stooq(code_cut_idx: int, n_code_cut: int = int(os.environ.get('N_CODE_CUT', 100))):
    df_stocklist = pd.read_csv('./stocklist_latest.csv')
    df_stocklist = df_stocklist.replace('-', np.nan)
    # TARGET_MARKETS = ['プライム（内国株式）', 'スタンダード（内国株式）', 'グロース（内国株式）']
    df_stocklist = df_stocklist[df_stocklist['市場・商品区分'].str.contains('内国株式')]

    s_qcut, bins = pd.qcut(df_stocklist['コード'].unique(), n_code_cut, labels=range(n_code_cut), retbins=True)
    # print(bins)
    df_cut = [df_stocklist[(df_stocklist['コード'] >= int(s_code)) & (df_stocklist['コード'] <= int(e_code))] for s_code, e_code in zip(bins[:-1], bins[1:])]
    print([len(df) for df in df_cut])

    target_codes = df_cut[code_cut_idx]['コード'].tolist()
    target_symbols = [f"{code}.JP" for code in target_codes]
    df = data.DataReader(target_symbols, 'stooq', start=date.today()-timedelta(days=4), end=date.today())
    if len(df) == 0:
        print(f'len(df) == 0 : {df.index.name}')
        return
    df.index.name = 'Date'
    print(df.reset_index())

    stockprices_to_insert_all = []

    print(f'target codes : {target_codes}')
    symbol_diff = set(target_symbols) - set(df['Close'].columns.unique())
    print(f'unable to fetch following symbols data : {symbol_diff}')

    sdt = datetime.now()
    for code in target_codes:
        if f'{code}.JP' not in df['Close'].columns:
            # print(f'skip {code}')
            continue
        stockprices = df.reset_index().apply(
            lambda x: to_stooq_stockprice_obj(code, x),
            axis=1,
        ).tolist()
        stockprices = [s for s in stockprices if s is not None]

        uniq_dates = [s.date for s in stockprices]

        alredy_exists_stockprices = db.query(
            StooqDailyStockpriceModel
        ).filter(
            (StooqDailyStockpriceModel.company_code == code) & (StooqDailyStockpriceModel.date.in_(uniq_dates))
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
