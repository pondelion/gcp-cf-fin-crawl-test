from datetime import datetime
import pandas as pd

from cf_src.rdb import (
    YFDailyStockpriceModel,
    SectorModel,
    CompanyModel,
    init_rdb,
    db,
)


init_rdb()


df_stocklist = pd.read_csv('./cf_src/stocklist_latest.csv')
df_stocklist = df_stocklist.loc[
    (df_stocklist['市場・商品区分'].str.contains('内国株式'))
    |
    (df_stocklist['市場・商品区分'].str.contains('ETF'))
]

uniq_sectors = sorted(df_stocklist['33業種区分'].unique().tolist())


sectors = [
    SectorModel(
        name=sec
    )
    for sec in uniq_sectors
]

sdt = datetime.now()
db.bulk_save_objects(sectors)
db.commit()
edt = datetime.now()
print(f'[sector bulk insert] took {(edt-sdt).total_seconds()}s')

sdt = datetime.now()
sector_name2id = {sector_name: SectorModel.query.filter(SectorModel.name==sector_name)[0].id for sector_name in uniq_sectors}
print(sector_name2id)
edt = datetime.now()
print(f'[select sector] took {(edt-sdt).total_seconds()}s')

uniq_codes = sorted(df_stocklist['コード'].unique().tolist())
sdt = datetime.now()
alredy_exists_companies = db.query(CompanyModel.code).filter(CompanyModel.code.in_(uniq_codes)).all()
edt = datetime.now()
print(f'[select company filter in uniq_codes] took {(edt-sdt).total_seconds()}s')
alredy_exists_company_codes = [c.code for c in alredy_exists_companies]

update_exclude_cols = ['created_at', 'updated_at']
row2dict = lambda row: {c.name: getattr(row, c.name) for c in row.__table__.columns if c.name not in update_exclude_cols}

companies_to_insert = [
    row2dict(CompanyModel(
        code=code,
        name=name,
        sector_id=sector_name2id[sector33],
        market=market,
    ))
    for code, name, market, sector33 in zip(df_stocklist['コード'], df_stocklist['銘柄名'], df_stocklist['市場・商品区分'], df_stocklist['33業種区分']) if code not in alredy_exists_company_codes
]
companies_to_update = [
    row2dict(CompanyModel(
        code=code,
        name=name,
        sector_id=sector_name2id[sector33],
        market=market,
    ))
    for code, name, market, sector33 in zip(df_stocklist['コード'], df_stocklist['銘柄名'], df_stocklist['市場・商品区分'], df_stocklist['33業種区分']) if code in alredy_exists_company_codes
]
print(f'len(companies_to_insert) : {len(companies_to_insert)}')
print(f'len(companies_to_update) : {len(companies_to_update)}')
print(companies_to_update[:2])
sdt = datetime.now()
if len(companies_to_insert) > 0:
    db.bulk_insert_mappings(CompanyModel, companies_to_insert)
if len(companies_to_update) > 0:
    db.bulk_update_mappings(CompanyModel, companies_to_update)
# db.bulk_save_objects(companies)
db.commit()
edt = datetime.now()
print(f'[companies bulk upsert] took {(edt-sdt).total_seconds()}s')

sdt = datetime.now()
companies_in_db = CompanyModel.query.all()
edt = datetime.now()
print(f'[select companies all] took {(edt-sdt).total_seconds()}s')
