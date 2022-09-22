from typing import Any
import os

from sqlalchemy.ext.declarative import as_declarative, declared_attr
from sqlalchemy import Boolean, Column, Date, DateTime, ForeignKey, Integer, String, BigInteger, Text, UniqueConstraint
from sqlalchemy.sql.functions import current_timestamp
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy_utils import database_exists, create_database, drop_database


DB_URI = os.environ['POSTGRES_DB_URI']


@as_declarative()
class Base:
    id: Any
    __name__: str
    __table_args__ = {'mysql_charset':'utf8mb4'}

    @declared_attr
    def __tablename__(cls) -> str:
        return f'{cls.__name__.lower()}'.replace('model', '')


class YFDailyStockpriceModel(Base):
    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True, nullable=False)
    date = Column(Date, index=True)
    open = Column(Integer, nullable=False)
    close = Column(Integer, nullable=False)
    high = Column(Integer, nullable=False)
    low = Column(Integer, nullable=True)
    adj_close = Column(Integer, nullable=False)
    volume = Column(Integer, nullable=False)
    company_code = Column(BigInteger, ForeignKey("company.code"), nullable=False)
    created_at = Column(
        DateTime,
        server_default=current_timestamp()
    )
    __table_args__ = (UniqueConstraint('date', 'company_code', name='unique_date_company_code'),)


class SectorModel(Base):
    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True, nullable=False)
    name = Column(Text, nullable=False)
    created_at = Column(
        DateTime,
        server_default=current_timestamp()
    )
    updated_at = Column(
        DateTime,
        server_default=current_timestamp(),
        onupdate=current_timestamp()
    )
    company = relationship("CompanyModel", back_populates="sector")


class CompanyModel(Base):
    code = Column(BigInteger, primary_key=True, index=True, autoincrement=False, nullable=False)
    name = Column(Text, nullable=False)
    sector_id = Column(BigInteger, ForeignKey("sector.id"), nullable=True)
    market = Column(Text, nullable=True)
    created_at = Column(
        DateTime,
        server_default=current_timestamp()
    )
    updated_at = Column(
        DateTime,
        server_default=current_timestamp(),
        onupdate=current_timestamp()
    )
    sector = relationship("SectorModel", back_populates="company")



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
    convert_unicode=True,
    pool_pre_ping=True
)

Session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=local_engine))
Base.query = Session.query_property()

db = Session()
print(local_engine.table_names())


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
        YFDailyStockpriceModel.__table__.drop(local_engine)
    except Exception as e:
        print(e)
    try:
        CompanyModel.__table__.drop(local_engine)
    except Exception as e:
        print(e)
    try:
        SectorModel.__table__.drop(local_engine)
    except Exception as e:
        print(e)
    try:
        Base.metadata.drop_all(local_engine)
    except Exception as e:
        print(e)

