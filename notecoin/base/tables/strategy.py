from notecoin.base.db import BaseTable
from sqlalchemy import (DATETIME, JSON, Column, Float, Identity, Integer,
                        String, Table, text)


class StrategyTable(BaseTable):

    def __init__(self, table_name="strategy", *args, **kwargs):
        super(StrategyTable, self).__init__(table_name, *args, **kwargs)
        self.table = Table(self.table_name, self.meta,
                           Column('id', Integer, Identity(start=42, cycle=True), comment='自增ID', primary_key=True),
                           Column('gmt_create', DATETIME, comment='创建时间', server_default=text('NOW()')),
                           Column('gmt_modified', DATETIME, comment='修改时间',
                                  server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP')),
                           Column('status', Integer, comment='状态'),
                           Column('ext_json', JSON, comment='拓展'),
                           Column('symbol', String(30), comment='symbol'),
                           Column('amount', Float, comment='amount'),
                           Column('buy_json', JSON, comment='买入信息'),
                           Column('sell_json', JSON, comment='卖出信息'),
                           Column('strategy1', JSON, comment='策略1'),

                           extend_existing=True,
                           # autoload=True
                           )
        self.create()
