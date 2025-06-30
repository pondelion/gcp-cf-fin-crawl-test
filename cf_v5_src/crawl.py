from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import numpy as np
import yfinance as yf

from rdb import (
    use_db_session,
    YFUS1DStockpriceModel,
    YFUS1DUpdateStatusModel,
)


@use_db_session
def crawl_yf(
    start_datetime = datetime.now()-timedelta(days=4),
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

    update_status = db.query(YFUS1DUpdateStatusModel).filter(
        YFUS1DUpdateStatusModel.crawl_timing_index == crawl_timing_index
    ).one_or_none()

    if update_status is None:
        update_status = YFUS1DUpdateStatusModel(
            crawl_timing_index=crawl_timing_index,
            last_succeeded=False,
            last_succeeded_at=None,
            last_crawled_at=None,
            ip=ip,
        )
        db.add(update_status)
    else:
        update_status.ip = ip

    # Define U.S. Eastern Time zone using standard library
    US_EASTERN = ZoneInfo("America/New_York")
    dt_now_est = datetime.now(US_EASTERN)
    # U.S. stock market closes at 4:00 PM EST
    if dt_now_est.hour >= 16:
        # Market is closed — fetch data up to today
        end_date = dt_now_est.date() + timedelta(days=1)  # yfinance's end date is exclusive
    else:
        # Market is still open — fetch data up to yesterday
        end_date = dt_now_est.date()
    print(f'Crawling data from {start_datetime.date()} to {end_date}, current EST: {dt_now_est}')
    df = yf.download(target_tickers, start=start_datetime.date(), end=end_date, interval='1d', auto_adjust=True)
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

        existing = {
            r.datetime.replace(tzinfo=None)
            for r in db.query(YFUS1DStockpriceModel.datetime)
                        .filter(YFUS1DStockpriceModel.ticker == ticker)
                        .filter(YFUS1DStockpriceModel.datetime >= df_t['datetime'].min())
                        .all()
        }

        for _, row in df_t.iterrows():
            dt = row['datetime'].to_pydatetime().replace(tzinfo=None)
            if dt in existing:
                continue

            obj = YFUS1DStockpriceModel(
                ticker=ticker,
                datetime=dt,
                open=row['Open'],
                high=row['High'],
                low=row['Low'],
                close=row['Close'],
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
