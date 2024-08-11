from funsecret import read_secret
from funsecret.sqlalchemy import BaseTable as BaseTable2
from funsecret.sqlalchemy import create_engine


class BaseTable(BaseTable2):
    def __init__(self, *args, **kwargs):
        uri = read_secret(cate1="funcoin", cate2="dataset", cate3="cron", cate4="uri")
        super(BaseTable, self).__init__(engine=create_engine(uri), *args, **kwargs)
