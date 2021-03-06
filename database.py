import pyodbc
import pymysql
from sqlalchemy import (
    create_engine,
    MetaData,
)
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from sqlalchemy.ext.declarative import declarative_base
from environment import (
    HIC_HOST,
    HIC_PASSWORD,
    HIC_USERNAME,
    MS_SQL_UHL_DWH_HOST,
    MS_SQL_UHL_DWH_USER,
    MS_SQL_UHL_DWH_PASSWORD,
    MS_SQL_ODBC_DRIVER,
    HIC_DB_USERNAME,
    HIC_DB_PASSWORD,
    HIC_DB_HOST,
    HIC_DB_DATABASE,
)

hic_covid_meta = MetaData()
Base = declarative_base(metadata=hic_covid_meta)


@contextmanager
def uhl_dwh_databases_engine():
    connectionstring = f'mssql+pyodbc://{MS_SQL_UHL_DWH_USER}:{MS_SQL_UHL_DWH_PASSWORD}@{MS_SQL_UHL_DWH_HOST}/dwbriccs?driver={MS_SQL_ODBC_DRIVER.replace(" ", "+")}'
    engine = create_engine(connectionstring)
    yield engine
    engine.dispose()


def uhl_dwh_connection_string():
    return f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={MS_SQL_UHL_DWH_HOST};DATABASE=dwbriccs;UID={MS_SQL_UHL_DWH_USER};PWD={MS_SQL_UHL_DWH_PASSWORD}'


def hic_connection_string():
    return f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={HIC_HOST};DATABASE=wh_hic_covid;UID={HIC_USERNAME};PWD={HIC_PASSWORD}'


@contextmanager
def uhl_dwh_conn():
    with pyodbc.connect(uhl_dwh_connection_string()) as con:
        yield con


@contextmanager
def hic_conn():
    with pyodbc.connect(hic_connection_string()) as con:
        yield con


@contextmanager
def hic_engine():
    connectionstring = f'mssql+pyodbc://{HIC_USERNAME}:{HIC_PASSWORD}@{HIC_HOST}/wh_hic_covid?driver={MS_SQL_ODBC_DRIVER.replace(" ", "+")}'
    engine = create_engine(connectionstring)
    yield engine
    engine.dispose()


@contextmanager
def hic_connection():
    db = pymysql.connect(
        host=HIC_DB_HOST,
        user=HIC_DB_USERNAME,
        password=HIC_DB_PASSWORD,
        database=HIC_DB_DATABASE,
    )

    try:
        yield db
    finally:
        db.close


@contextmanager
def hic_cursor():
    with hic_connection() as conn:
        yield conn.cursor(pymysql.cursors.SSCursor)


@contextmanager
def hic_covid_session():
    try:
        engine = create_engine(f'mysql+pymysql://{HIC_DB_USERNAME}:{HIC_DB_PASSWORD}@{HIC_DB_HOST}/{HIC_DB_DATABASE}')
        hic_covid_meta.bind = engine
        session_maker = sessionmaker(bind=engine)
        session = session_maker()
        yield session

    except Exception as e:
        session.rollback()
        session.close()
        raise e
    else:
        session.commit()
        session.close()
    finally:
        engine.dispose
