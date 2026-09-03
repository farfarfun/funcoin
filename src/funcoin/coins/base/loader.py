import csv

import ccxt
import orjson
import pandas as pd
from ccxt.base.exchange import Exchange
from farlog import getLogger
from tqdm import tqdm

logger = getLogger("funcoin")
unix_month = 2678400000
one_hour = 3600 * 1000


class BaseLoader:
    """行情/成交数据加载器基类。

    子类通过实现 `_open`/`_write`/`_close`/`_load_symbols`/`_load_symbol`
    这几个 hook 方法来定制数据的落地方式（如写 CSV、写数据库等）。
    """

    def __init__(self, unix_start: int, unix_end: int, *args, **kwargs) -> None:
        """
        Args:
            unix_start: 拉取数据的起始时间（毫秒级 unix 时间戳）。
            unix_end: 拉取数据的结束时间（毫秒级 unix 时间戳）。
        """
        self.unix_start = unix_start
        self.unix_end = unix_end
        self.cache_data: list = []

    def _open(self, *args, **kwargs) -> None:
        pass

    def _write(self, data_list: list) -> None:
        pass

    def _close(self, *args, **kwargs) -> None:
        pass

    def _load_symbols(self, *args, **kwargs) -> None:
        pass

    def _load_symbol(self, symbol: str, pbr=None, *args, **kwargs) -> None:
        pass

    def load_symbols(self, *args, **kwargs) -> None:
        """加载全部交易对的数据。"""
        self._open(*args, **kwargs)
        self._load_symbols(*args, **kwargs)
        self._close(*args, **kwargs)

    def load_symbol(self, symbol: str, pbr=None, *args, **kwargs) -> None:
        """加载单个交易对的数据。

        Args:
            symbol: 交易对名称，如 `BTC/USDT`。
            pbr: 可选的进度条对象，用于更新描述信息。
        """
        self._open(*args, **kwargs)
        self._load_symbol(symbol=symbol, pbr=pbr, *args, **kwargs)
        self._close(*args, **kwargs)

    def write_data(self, data_list: list, cache: bool = True) -> None:
        """缓存并按需落盘写入数据。

        Args:
            data_list: 待写入的数据记录列表。
            cache: 为 True 时先攒批，缓存量小于 10000 条暂不落盘；
                为 False 时强制立即落盘（用于收尾时 flush 剩余数据）。
        """
        self.cache_data.extend(data_list)
        if cache and len(self.cache_data) < 10000:
            return
        if len(self.cache_data) == 0:
            return
        df = pd.DataFrame(self.cache_data)
        df = df[
            (df["timestamp"] >= self.unix_start) & (df["timestamp"] <= self.unix_end)
        ]
        self._write(orjson.loads(df.to_json(orient="records")))
        self.cache_data.clear()

    def __enter__(self) -> "BaseLoader":
        self._handle = self
        self._open()
        return self._handle

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        # 无论是否发生异常，都要先 flush 剩余缓存数据并关闭资源；
        # 但不能吞掉调用方 with 块里的异常，所以清理完毕后返回 False，
        # 让异常正常向上传播。
        if self.cache_data is not None:
            self.write_data([], cache=False)
        self._close()
        return False


class CSVLoader(BaseLoader):
    """把数据写成 CSV 文件的加载器。"""

    def __init__(self, csv_path: str, fieldnames: list, *args, **kwargs) -> None:
        """
        Args:
            csv_path: 输出 CSV 文件路径。
            fieldnames: CSV 列名列表。
        """
        self.csv_path = csv_path
        super().__init__(*args, **kwargs)
        self.csv_file = open(self.csv_path, mode="w")
        self.csv_writer = csv.DictWriter(
            self.csv_file, delimiter=",", fieldnames=fieldnames
        )
        self.csv_writer.writeheader()

    def _write(self, data_list: list) -> None:
        self.csv_writer.writerows(data_list)
        self.csv_file.flush()

    def _close(self, *args, **kwargs) -> None:
        self.csv_file.close()


class CCXTBaseLoader(CSVLoader):
    """基于 ccxt 交易所客户端遍历全部交易对的加载器基类。"""

    def __init__(self, exchange: Exchange, *args, **kwargs) -> None:
        """
        Args:
            exchange: 已配置好的 ccxt 交易所客户端实例。
        """
        self.exchange = exchange
        super().__init__(*args, **kwargs)
        self.exchange.load_markets()

    def _load_symbols(self, *args, **kwargs) -> None:
        pbr = tqdm(self.exchange.symbols)
        for sym in pbr:
            if ":" not in sym:
                pbr.set_description(sym)
                self._load_symbol(sym, pbr, *args, **kwargs)
        self.write_data([], False)


class KlineLoder(CCXTBaseLoader):
    """K 线（OHLCV）数据加载器。"""

    def __init__(self, *args, timeframe: str = "1m", **kwargs) -> None:
        """
        Args:
            timeframe: K 线周期，如 `1m`、`1h`。
        """
        super(KlineLoder, self).__init__(
            fieldnames=["symbol", "timestamp", "open", "close", "low", "high", "vol"],
            *args,
            **kwargs,
        )
        self.timeframe = timeframe

    def _load_symbol(self, symbol: str, pbr=None, *args, **kwargs) -> None:
        unix_temp = self.unix_start
        for _ in range(1000):
            if unix_temp >= self.unix_end:
                break
            try:
                result = self.exchange.fetch_ohlcv(
                    symbol, self.timeframe, unix_temp, limit=500
                )
                result = self.exchange.sort_by(result, 0)
                if len(result) == 0:
                    break
                unix_temp = result[-1][0]
                df = pd.DataFrame(
                    result, columns=["timestamp", "open", "close", "low", "high", "vol"]
                )
                df["symbol"] = symbol
                self.write_data(orjson.loads(df.to_json(orient="records")))
                # time.sleep(int(self.exchange.rateLimit / 1000))
            except Exception as e:
                logger.error(
                    f"拉取K线失败 exchange={self.exchange.id} symbol={symbol} "
                    f"timeframe={self.timeframe} since={unix_temp}: {e}"
                )
                self.exchange.sleep(1000)


class TradeLoader(CCXTBaseLoader):
    """逐笔成交数据加载器。"""

    def __init__(self, *args, **kwargs) -> None:
        super(TradeLoader, self).__init__(
            fieldnames=["symbol", "id", "timestamp", "side", "price", "amount"],
            *args,
            **kwargs,
        )

    def _load_symbol(self, symbol: str, pbr=None, *args, **kwargs) -> None:
        unix_temp = self.unix_start
        previous_trade_id = None
        for _ in range(10000):
            pbr.set_description(f"{symbol}-{unix_temp}")
            if unix_temp >= self.unix_end:
                break
            try:
                trades = self.exchange.fetch_trades(symbol, unix_temp, limit=1000)
                if len(trades) == 0:
                    unix_temp += one_hour
                    continue
                last_trade = trades[-1]
                if previous_trade_id == last_trade["id"]:
                    unix_temp += one_hour
                    continue
                unix_temp = last_trade["timestamp"]
                previous_trade_id = last_trade["id"]

                result = [
                    {
                        "symbol": trade["symbol"],
                        "id": trade["id"].replace("\n", ""),
                        "timestamp": int(trade["timestamp"]),
                        "side": trade["side"][0],
                        "price": trade["price"],
                        "amount": trade["amount"],
                    }
                    for trade in trades
                ]

                self.write_data(result)
                # time.sleep(int(self.exchange.rateLimit / 1000))
            except ccxt.NetworkError as e:
                logger.error(
                    f"拉取成交记录失败 exchange={self.exchange.id} symbol={symbol} "
                    f"since={unix_temp}: {e}"
                )
                self.exchange.sleep(1000)
