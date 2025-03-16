from datetime import date, datetime, timedelta, timezone
import os

import pandas as pd
from pandas_datareader import data
import numpy as np
import yfinance as yf

from rdb import (
    db,
    YFDailyStockpriceModel,
    StooqDailyStockpriceModel,
    StooqHourlyUpdateStatusModel,
    YahooHourlyUpdateStatusModel,
)


INT_MAX = 2147483647-1
INT_MIN = -2147483648+1
g_yf_overrided = False


def to_yf_stockprice_obj(code, sr_stockprice):
    # if sr_stockprice.isnull().any():
    #     return None
    high = sr_stockprice[('High', f'{code}.T')]
    low = sr_stockprice[('Low', f'{code}.T')]
    open = sr_stockprice[('Open', f'{code}.T')]
    close = sr_stockprice[('Close', f'{code}.T')]
    volume = sr_stockprice[('Volume', f'{code}.T')]
    adj_close = 0  # sr_stockprice[('Adj Close', f'{code}.T')]
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
        open=int(np.clip(round(open), INT_MIN, INT_MAX)),
        close=int(np.clip(round(close), INT_MIN, INT_MAX)),
        high=int(np.clip(round(high), INT_MIN, INT_MAX)),
        low=int(np.clip(round(low), INT_MIN, INT_MAX)),
        volume=int(np.clip(round(volume), INT_MIN, INT_MAX)),
        company_code=code,
    )
    return stockprice


def crawl_yf(
    code_cut_idx: int,
    start_date = date.today()-timedelta(days=4),
    ip: str = None,
):
    # import yfinance as yf
    # yf.pdr_override()

    df_stocklist = pd.read_csv('./stocklist_latest.csv')
    df_stocklist = df_stocklist.replace('-', np.nan)
    # TARGET_MARKETS = ['プライム（内国株式）', 'スタンダード（内国株式）', 'グロース（内国株式）']
    df_stocklist = df_stocklist.loc[
        (df_stocklist['市場・商品区分'].str.contains('内国株式'))
        |
        (df_stocklist['市場・商品区分'].str.contains('ETF'))
    ]

    df_stocklist_tgt = df_stocklist.loc[df_stocklist['crawl_hour_jst'] == code_cut_idx]
    if len(df_stocklist_tgt) == 0:
        print(f'no crawl target for crawl_hour_jst = {code_cut_idx}')
        return
    print(code_cut_idx, df_stocklist_tgt['市場・商品区分'].value_counts())
    target_codes = df_stocklist_tgt['コード'].tolist()
    target_symbols = [f"{code}.T" for code in target_codes]
    print(f'target codes : {target_codes}')

    update_status = db.query(YahooHourlyUpdateStatusModel).filter(YahooHourlyUpdateStatusModel.code_cut_index == code_cut_idx).all()
    assert len(update_status) == 1, len(update_status)
    update_status = update_status[0]
    update_status.ip = ip

    JST = timezone(timedelta(hours=+9), 'JST')
    dt_now_jst = datetime.now(JST)
    if dt_now_jst.hour >= 17:
        # today's market closed, fetch data upto today
        end_date = dt_now_jst.date() + timedelta(days=1)  # yahoo api returns max date = end_day-1day(=today)
    else:
        # today's market not closed, fetch data upto yesterday
        end_date = dt_now_jst.date()  # yahoo api returns max date = end_day-1day(=yesterday)
    # end_date = dt_now_jst.date()
    print(f'crawling {start_date} - {end_date}, dt_now_jst : {dt_now_jst}')
    df = yf.download(target_symbols, start=start_date, end=end_date, auto_adjust=True)
    if len(df) == 0:
        print(f'len(df) == 0 : {df.index.name}')
        update_status.last_succeeded = False
        update_status.last_crawled_at = datetime.now()
        db.commit()
        db.close()
        return
    df.index.name = 'Date'
    print(f'df.index.max() : {df.index.max()}')
    print(df.reset_index())
    if (dt_now_jst.hour < 16) and (df.index.max() == end_date):
        print('[WARNING] (dt_now_jst.hour < 16) and (df.index.max() == end_date)')
    if dt_now_jst.hour >= 17:
        # today's market closed, retain data upto today
        max_date_to_retain = dt_now_jst.date()
    else:
        # today's market not closed, retain data upto yesterday
        max_date_to_retain = dt_now_jst.date() - timedelta(days=1)
    df = df[df.index.map(lambda x: x.date()) <= max_date_to_retain]
    print(f'df.index.max() after filter : {df.index.max()}')

    stockprices_to_insert_all = []

    symbol_diff = set(target_symbols) - set(df['Close'].columns.unique())
    THRESH_FAIL = 10
    crawl_succeded = len(symbol_diff) < THRESH_FAIL
    print(f'unable to fetch following symbols data : {symbol_diff}')
    print(f'crawl_succeded => {crawl_succeded}')

    sdt = datetime.now()
    for code in target_codes:
        if f'{code}.T' not in df['Close'].columns:
            # print(f'skip {code}')
            continue
        stockprices = df.reset_index().apply(
            lambda x: to_yf_stockprice_obj(code, x),
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

    update_status.last_succeeded = crawl_succeded
    update_status.last_crawled_at = sdt
    if crawl_succeded:
        update_status.last_succeeded_at = sdt
    db.commit()
    db.close()


# def crawl_stooq(
#     code_cut_idx: int,
#     n_code_cut: int = int(os.environ.get('N_CODE_CUT', 100)),
#     start_date = date.today()-timedelta(days=4),
#     ip: str = None,
# ):
#     df_stocklist = pd.read_csv('./stocklist_latest.csv')
#     df_stocklist = df_stocklist.replace('-', np.nan)
#     # TARGET_MARKETS = ['プライム（内国株式）', 'スタンダード（内国株式）', 'グロース（内国株式）']
#     df_stocklist = df_stocklist.loc[
#         (df_stocklist['市場・商品区分'].str.contains('内国株式'))
#         |
#         (df_stocklist['市場・商品区分'].str.contains('ETF'))
#     ]

#     s_qcut, bins = pd.qcut(df_stocklist['コード'].unique(), n_code_cut, labels=range(n_code_cut), retbins=True)
#     # print(bins)
#     df_cut = [df_stocklist[(df_stocklist['コード'] >= int(s_code)) & (df_stocklist['コード'] <= int(e_code))] for s_code, e_code in zip(bins[:-1], bins[1:])]
#     print([len(df) for df in df_cut])

#     target_codes = df_cut[code_cut_idx]['コード'].tolist()
#     target_symbols = [f"{code}.JP" for code in target_codes]
#     print(f'target codes : {target_codes}')

#     update_status = db.query(StooqHourlyUpdateStatusModel).filter(StooqHourlyUpdateStatusModel.code_cut_index == code_cut_idx).all()
#     assert len(update_status) == 1, len(update_status)
#     update_status = update_status[0]
#     update_status.ip = ip

#     JST = timezone(timedelta(hours=+9), 'JST')
#     dt_now_jst = datetime.now(JST)
#     # if dt_now_jst.hour >= 17:
#     #     # today's market closed, fetch data upto today
#     #     end_date = dt_now_jst.date()
#     # else:
#     #     # today's market not closed, fetch data upto yesterday
#     #     end_date = dt_now_jst.date() - timedelta(days=1)
#     end_date = dt_now_jst.date()
#     df = data.DataReader(target_symbols, 'stooq', start=start_date, end=end_date)
#     if len(df) == 0:
#         print(f'len(df) == 0 : {df.index.name}')
#         update_status.last_succeeded = False
#         update_status.last_crawled_at = datetime.now()
#         db.commit()
#         db.close()
#         return
#     df.index.name = 'Date'
#     print(df.reset_index())

#     stockprices_to_insert_all = []

#     symbol_diff = set(target_symbols) - set(df['Close'].columns.unique())
#     THRESH_FAIL = 10
#     crawl_succeded = len(symbol_diff) < THRESH_FAIL
#     print(f'unable to fetch following symbols data : {symbol_diff}')
#     print(f'crawl_succeded => {crawl_succeded}')

#     sdt = datetime.now()
#     for code in target_codes:
#         if f'{code}.JP' not in df['Close'].columns:
#             # print(f'skip {code}')
#             continue
#         stockprices = df.reset_index().apply(
#             lambda x: to_stooq_stockprice_obj(code, x),
#             axis=1,
#         ).tolist()
#         stockprices = [s for s in stockprices if s is not None]

#         uniq_dates = [s.date for s in stockprices]

#         alredy_exists_stockprices = db.query(
#             StooqDailyStockpriceModel
#         ).filter(
#             (StooqDailyStockpriceModel.company_code == code) & (StooqDailyStockpriceModel.date.in_(uniq_dates))
#         ).all()

#         alredy_exists_stockprice_dates = [s.date for s in alredy_exists_stockprices]

#         stockprices_to_insert = [s for s in stockprices if s.date.date() not in alredy_exists_stockprice_dates]

#         stockprices_to_insert_all += stockprices_to_insert

#     edt = datetime.now()
#     print(f'[select stockprice filter in uniq_dates] took {(edt-sdt).total_seconds()}s')

#     print(f'len(stockprices_to_insert) : {len(stockprices_to_insert_all)}')

#     if len(stockprices_to_insert_all) > 0:
#         sdt = datetime.now()
#         db.bulk_save_objects(stockprices_to_insert_all)
#         db.commit()
#         edt = datetime.now()
#         print(f'[stockprice bulk insert] took {(edt-sdt).total_seconds()}s')

#     update_status.last_succeeded = crawl_succeded
#     update_status.last_crawled_at = sdt
#     if crawl_succeded:
#         update_status.last_succeeded_at = sdt
#     db.commit()
#     db.close()
