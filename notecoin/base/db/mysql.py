import pymysql
from notesecret import read_secret
from pymysql.cursors import DictCursor
from sqlalchemy import MetaData, Table, create_engine, delete, inspect, select
from sqlalchemy.dialects.mysql import insert

engine_dict = {}
meta_dict = {}


class create_conn(object):
    def __init__(self, database=None, *args, **kwargs):
        self.database = database
        self.instance = None

    def __enter__(self):
        self.instance = pymysql.connect(
            host=read_secret(cate1='notecoin', cate2='dataset', cate3='mysql', cate4='host'),
            user=read_secret(cate1='notecoin', cate2='dataset', cate3='mysql', cate4='user'),
            password=read_secret(cate1='notecoin', cate2='dataset', cate3='mysql', cate4='password'),
            database=self.database,
            charset="utf8mb4",
            cursorclass=DictCursor)
        self._handle = self.instance.cursor()
        return self._handle

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._handle.close()
        self.instance.close()


def create_database(db_name):
    with create_conn() as conn:
        conn.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")


def _get_uri(key=None):
    host = read_secret(cate1='notecoin', cate2='dataset', cate3='mysql', cate4='host')
    port = read_secret(cate1='notecoin', cate2='dataset', cate3='mysql', cate4='port') or '3306'
    user = read_secret(cate1='notecoin', cate2='dataset', cate3='mysql', cate4='user')
    password = read_secret(cate1='notecoin', cate2='dataset', cate3='mysql', cate4='password')
    db_name = 'notecoin' if key is None or len(key) == 0 else f'notecoin_{key.lower()}'
    create_database(db_name)
    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}?charset=utf8"


def _init_engine_meta(key=None):
    engine = create_engine(_get_uri(key=key))
    engine_dict[key] = engine
    meta_dict[key] = MetaData(bind=engine)


def get_engine(key=None):
    if key not in engine_dict.keys():
        _init_engine_meta(key)
    return engine_dict[key]


def get_meta(key=None):
    if key not in meta_dict.keys():
        _init_engine_meta(key)
    return meta_dict[key]


class BaseTable:
    def __init__(self, table_name, db_suffix=None, *args, **kwargs):
        self.table_name = table_name
        self.table: Table = None
        self.engine = get_engine(db_suffix)
        self.meta = get_meta(db_suffix)

    def create(self):
        self.meta.create_all(self.engine)

    def select_all(self):
        return self.engine.execute(select([self.table]))

    def delete_all(self):
        return self.engine.execute(delete(self.table))

    def upsert(self, value):
        stmt = insert(self.table).values([value])
        primary_keys = [key.name for key in inspect(self.table).primary_key]
        update_dict = {c.name: c for c in stmt.inserted if
                       not c.primary_key and c.name not in ('gmt_create', 'gmt_modified') and c.name in value.keys() and value[c.name] is not None }
        if len(update_dict) == 0:
            return
        stmt = stmt.on_duplicate_key_update(update_dict)
        self.engine.execute(stmt)
