from funcoin.base.db.mysql import BaseTable
from sqlalchemy import BIGINT, Column, Float, String, Table, func, select


class TradeData(BaseTable):
    def __init__(self, table_name="trade_data", prefix="com", suffix="com", *args, **kwargs):
        table_name = f"{prefix.lower()}_{table_name}_{suffix.lower()}"
        super(TradeData, self).__init__(table_name=table_name, db_suffix=prefix.lower(), *args, **kwargs)
        self.table = Table(
            self.table_name,
            self.meta,
            Column("symbol", String(30), comment="symbol", primary_key=True),
            Column("id", String(50), comment="trade_id", primary_key=True),
            Column("timestamp", BIGINT, comment="timestamp", default=0),
            Column("type", String(10), comment="order type, 'market', 'limit'", default=""),
            Column("side", String(10), comment="direction of the trade, 'buy' or 'sell'", default=""),
            Column("price", Float, comment="price", default=0),
            Column("amount", Float, comment="amount", default=0),
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
