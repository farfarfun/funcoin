from funcoin.base.db.mysql import BaseTable
from sqlalchemy import BIGINT, Column, Float, String, Table, func, select


class KlineData(BaseTable):
    def __init__(self, table_name="kline_data_1min", prefix="com", suffix="com", *args, **kwargs):
        table_name = f"{prefix.lower()}_{table_name}_{suffix.lower()}"

        super(KlineData, self).__init__(table_name=table_name, db_suffix=prefix.lower(), *args, **kwargs)
        self.table = Table(
            self.table_name,
            self.meta,
            Column("symbol", String(30), comment="symbol", primary_key=True),
            Column("timestamp", BIGINT, comment="timestamp", primary_key=True),
            Column("open", Float, comment="open", default=0),
            Column("close", Float, comment="close", default=0),
            Column("low", Float, comment="low", default=0),
            Column("high", Float, comment="high", default=0),
            Column("vol", Float, comment="vol", default=0),
            extend_existing=True,
            # autoload=True
        )
        self.create()

    def select_symbol_maxmin(self, symbol):
        self.create()
        s = (
            select(
                self.table.columns.symbol,
                func.max(self.table.columns.timestamp).label("max_time"),
                func.min(self.table.columns.timestamp).label("min_time"),
            )
            .where(self.table.columns.symbol == symbol)
            .group_by(self.table.columns.symbol)
        )
        with self.engine.connect() as conn:
            data = [line for line in conn.execute(s)]
            if len(data) == 1:
                return data[0][1], data[0][2]
            else:
                return 0, 0
