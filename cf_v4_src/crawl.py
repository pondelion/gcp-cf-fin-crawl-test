from datetime import date, datetime, timedelta, timezone
import os

import pandas as pd
import numpy as np
import yfinance as yf

from rdb import (
    use_db_session,
    YFUS1HStockpriceModel,
    YFUS1HUpdateStatusModel,
)


@use_db_session
def crawl_yf(
    start_datetime = datetime.now()-timedelta(hours=6),
    ip: str = None,
    crawl_timing_index: int = datetime.now().hour,
    db = None,
):

    df_tickerlist = pd.read_csv('./ticker_list.csv')

    # df_stocklist_tgt = df_tickerlist.loc[df_tickerlist['crawl_hour_utc'] == crawl_timing_index]
    df_stocklist_tgt = df_tickerlist
    if len(df_stocklist_tgt) == 0:
        print(f'no crawl target for crawl_hour_crawl_hour_utc = {crawl_timing_index}')
        return
    target_tickers = df_stocklist_tgt['ticker'].tolist()
    print(f'target tickers : {target_tickers}')

    update_status = db.query(YFUS1HUpdateStatusModel).filter(
        YFUS1HUpdateStatusModel.crawl_timing_index == crawl_timing_index
    ).one_or_none()

    if update_status is None:
        update_status = YFUS1HUpdateStatusModel(
            crawl_timing_index=crawl_timing_index,
            last_succeeded=False,
            last_succeeded_at=None,
            last_crawled_at=None,
            ip=ip,
        )
        db.add(update_status)
    else:
        update_status.ip = ip

    dt_now_utc = datetime.now(timezone.utc)
    def floor_to_1h_from_30(dt):
        dt_floor = dt.replace(minute=30, second=0, microsecond=0)
        if dt.minute < 30:
            dt_floor = dt_floor - timedelta(hours=1)
        return dt_floor
    # dt_now_utc.replace(minute=0, second=0, microsecond=0)
    end_datetime = floor_to_1h_from_30(dt_now_utc)  # floor to previous XX:30
    print(f'crawling {start_datetime} - {end_datetime}, dt_now_utc : {dt_now_utc}')
    df = yf.download(target_tickers, start=start_datetime, end=end_datetime, interval='1h', auto_adjust=True)
    if len(df) == 0:
        print(f'len(df) == 0 : {df.index.name}')
        update_status.last_succeeded = False
        update_status.last_crawled_at = datetime.now()
        db.commit()
        # db.close()
        return
    df.index.name = 'datetime'
    print(f'df.index.max() : {df.index.max()}')
    print(df.reset_index())

    stockprices_to_insert_all = []

    fetched_symbols = df['Close'].columns.unique().tolist()
    symbol_diff = set(target_tickers) - set(fetched_symbols)
    THRESH_FAIL = 10
    crawl_succeeded = len(symbol_diff) < THRESH_FAIL
    print(f'unable to fetch following symbols data : {symbol_diff}')
    print(f'crawl_succeeded => {crawl_succeeded}')

    df = df.swaplevel(axis=1).sort_index(axis=1)  # (OHLCV, ticker) -> (ticker, OHLCV)
    for ticker in target_tickers:
        if ticker not in fetched_symbols:
            continue

        df_t = df[ticker].reset_index()  # Open/High/Low/Close/Volume

        # 既存レコード取得
        existing = {
            r.datetime.replace(tzinfo=None)
            for r in db.query(YFUS1HStockpriceModel.datetime)
                        .filter(YFUS1HStockpriceModel.ticker == ticker)
                        .filter(YFUS1HStockpriceModel.datetime >= df_t['datetime'].min())
                        .all()
        }

        for _, row in df_t.iterrows():
            dt = row['datetime'].to_pydatetime().replace(tzinfo=None)
            if dt in existing:
                continue

            obj = YFUS1HStockpriceModel(
                ticker=ticker,
                datetime=dt,
                open=int(row['Open']),
                high=int(row['High']),
                low=int(row['Low']),
                close=int(row['Close']),
                volume=int(row['Volume']),
            )
            stockprices_to_insert_all.append(obj)

    print(f'len(stockprices_to_insert_all): {len(stockprices_to_insert_all)}')

    if stockprices_to_insert_all:
        sdt = datetime.now()
        db.bulk_save_objects(stockprices_to_insert_all)
        db.commit()
        edt = datetime.now()
        print(f'[stockprice bulk insert] took {(edt - sdt).total_seconds()}s')

    update_status.last_succeeded = crawl_succeeded
    update_status.last_crawled_at = datetime.now()
    if crawl_succeeded:
        update_status.last_succeeded_at = datetime.now()
    db.commit()
    # db.close()
