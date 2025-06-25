from typing import Any
import os
from functools import wraps

from sqlalchemy.ext.declarative import as_declarative, declared_attr
from sqlalchemy import Boolean, Column, Date, DateTime, ForeignKey, Integer, String, BigInteger, Text, UniqueConstraint
from sqlalchemy.sql import func
from sqlalchemy.sql.functions import current_timestamp
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy_utils import database_exists, create_database, drop_database


DB_URI = os.environ['DB_URI']


@as_declarative()
class Base:
    id: Any
    __name__: str
    __table_args__ = {'mysql_charset':'utf8mb4'}

    # @declared_attr
    # def __tablename__(cls) -> str:
    #     return f'{cls.__name__.lower()}'.replace('model', '')


class YFUS1HStockpriceModel(Base):
    __tablename__ = 'yf_us_1h_stockprice'

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True, nullable=False)
    ticker = Column(String(10), index=True, nullable=False)  # e.g., "SPY", "QQQ"
    datetime = Column(DateTime, index=True, nullable=False)
    open = Column(Integer, nullable=False)
    close = Column(Integer, nullable=False)
    high = Column(Integer, nullable=False)
    low = Column(Integer, nullable=False)
    volume = Column(Integer, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        UniqueConstraint('datetime', 'ticker', name='unique_datetime_ticker'),
    )


class YFUS1HUpdateStatusModel(Base):
    __tablename__ = 'yf_us_1h_update_status'

    crawl_timing_index = Column(Integer, primary_key=True, nullable=False, unique=True)
    last_succeeded = Column(Boolean, nullable=False)
    last_succeeded_at = Column(DateTime, nullable=True)
    last_crawled_at = Column(DateTime, nullable=True)
    ip = Column(Text, nullable=True)


def delete_database() -> None:
    if database_exists(DB_URI):
        print(f'Deleting database : {DB_URI}')
        drop_database(DB_URI)


def create_databse() -> None:
    if not database_exists(DB_URI):
        print(f'Database not found. Creating database : {DB_URI}')
        create_database(DB_URI)


local_engine = create_engine(
    DB_URI,
    # convert_unicode=True,
    pool_pre_ping=True
)

Session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=local_engine))
Base.query = Session.query_property()

# db = Session()
def use_db_session(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        db = Session()
        try:
            return func(*args, db=db, **kwargs)
        except Exception as e:
            db.rollback()
            raise e
        finally:
            db.close()
    return wrapper
# print(local_engine.table_names())


def init_rdb(
    recreate_database: bool = False,
    recreate_table: bool = False
) -> None:
    if recreate_database:
        delete_database()
    create_databse()
    if recreate_table:
        drop_tables()
    Base.metadata.create_all(local_engine)


def show_tables() -> None:
    print(local_engine.table_names())


def drop_tables() -> None:
    # Base.metadata.drop_all(local_engine)
    try:
        YFUS1HStockpriceModel.__table__.drop(local_engine)
    except Exception as e:
        print(e)
    try:
        YFUS1HUpdateStatusModel.__table__.drop(local_engine)
    except Exception as e:
        print(e)
    try:
        Base.metadata.drop_all(local_engine)
    except Exception as e:
        print(e)

