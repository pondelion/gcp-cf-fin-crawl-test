from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import yfinance as yf
from sqlalchemy.dialects.postgresql import insert

from rdb import (
    use_db_session,
    YFUS1DStockpriceModel,
    YFUS1DUpdateStatusModel,
)


def upsert_stockprice(db, obj):
    stmt = insert(YFUS1DStockpriceModel).values(
        ticker=obj.ticker,
        datetime=obj.datetime,
        open=obj.open,
        close=obj.close,
        high=obj.high,
        low=obj.low,
        volume=obj.volume,
    )
    update_dict = {
        'open': obj.open,
        'close': obj.close,
        'high': obj.high,
        'low': obj.low,
        'volume': obj.volume,
    }
    stmt = stmt.on_conflict_do_update(
        index_elements=['datetime', 'ticker'],
        set_=update_dict,
    )
    db.execute(stmt)


@use_db_session
def crawl_yf(
    start_datetime = datetime.now()-timedelta(days=4),
    ip: str = None,
    crawl_timing_index: int = datetime.now().hour,
    db = None,
):
    if crawl_timing_index == 22:
        target_type = 'ETF'
    elif crawl_timing_index == 23:
        target_type = 'INDICATOR'

    df_etf_tickerlist = pd.read_csv('./etf_ticker_list.csv')
    df_etf_tickerlist['crawl_hour_utc'] = 22
    df_indicator_tickerlist = pd.read_csv('./indicator_ticker_list.csv')
    df_indicator_tickerlist['crawl_hour_utc'] = 23
    df_tickerlist = pd.concat([df_etf_tickerlist, df_indicator_tickerlist])

    df_stocklist_tgt = df_tickerlist.loc[df_tickerlist['crawl_hour_utc'] == crawl_timing_index]
    # df_stocklist_tgt = df_tickerlist
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
    if target_type == 'ETF':
        # U.S. stock market closes at 4:00 PM EST
        if dt_now_est.hour >= 16:
            # Market is closed — fetch data up to today
            end_date = dt_now_est.date() + timedelta(days=1)  # yfinance's end date is exclusive
        else:
            # Market is still open — fetch data up to yesterday
            end_date = dt_now_est.date()
    elif target_type == 'INDICATOR':
        end_date = date.today() + timedelta(days=1)  # Crawl data up to today, and remove the most recent row afterward
    print(f'Crawling data from {start_datetime.date()} to {end_date}, current EST: {dt_now_est}')
    df = yf.download(target_tickers, start=start_datetime.date(), end=end_date, interval='1d', auto_adjust=True)
    if len(df) == 0:
        print(f'len(df) == 0 : {df.index.name}')
        update_status.last_succeeded = False
        update_status.last_crawled_at = datetime.now()
        db.commit()
        # db.close()
        return
    if target_type == 'INDICATOR':
        # remove the most recent valid(non-null) row
        def remove_valid_latest_1d_row(df_ticker):
            df_ticker = df_ticker.sort_index()
            nan_cnt = df_ticker.bfill()['Close'].isnull().sum()
            df_ticker = df_ticker.iloc[:max(0, len(df_ticker) - (nan_cnt + 1))]
            return df_ticker
        df = df.stack('Ticker').groupby('Ticker', group_keys=False).apply(remove_valid_latest_1d_row).unstack('Ticker').sort_index()
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

    save_mode = 'upsert'  # 'upsert' or 'insert'

    df = df.swaplevel(axis=1).sort_index(axis=1)  # (OHLCV, ticker) -> (ticker, OHLCV)
    for ticker in target_tickers:
        if ticker not in fetched_symbols:
            continue

        df_t = df[ticker].reset_index()  # Open/High/Low/Close/Volume
        df_t = df_t.dropna(subset=['Close', 'High', 'Low', 'Open'])
        if len(df_t) == 0:
            print(f'{ticker} len=0 after dropna')
            continue
        df_t = df_t.fillna(0)

        existing = {
            r.datetime.replace(tzinfo=None)
            for r in db.query(YFUS1DStockpriceModel.datetime)
                        .filter(YFUS1DStockpriceModel.ticker == ticker)
                        .filter(YFUS1DStockpriceModel.datetime >= df_t['datetime'].min())
                        .all()
        }

        for _, row in df_t.iterrows():
            dt = row['datetime'].to_pydatetime().replace(tzinfo=None)
            if save_mode == 'insert' and dt in existing:
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
            if save_mode == 'insert':
                stockprices_to_insert_all.append(obj)
            elif save_mode == 'upsert':
                upsert_stockprice(db, obj)
            else :
                raise ValueError(save_mode)

    print(f'len(stockprices_to_insert_all): {len(stockprices_to_insert_all)}')

    if save_mode == 'insert' and stockprices_to_insert_all:
        sdt = datetime.now()
        db.bulk_save_objects(stockprices_to_insert_all)
        db.commit()
        edt = datetime.now()
        print(f'[stockprice bulk insert] took {(edt - sdt).total_seconds()}s')
    elif save_mode == 'upsert':
        db.commit()

    update_status.last_succeeded = crawl_succeeded
    update_status.last_crawled_at = datetime.now()
    if crawl_succeeded:
        update_status.last_succeeded_at = datetime.now()
    db.commit()
    # db.close()
