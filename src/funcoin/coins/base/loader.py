import csv

import ccxt
import pandas as pd
from ccxt.base.exchange import Exchange
from farlog import getLogger
from tqdm import tqdm

logger = getLogger("funcoin")
unix_month = 2678400000
one_hour = 3600 * 1000

# ccxt `fetch_ohlcv()` 的返回契约，列顺序固定为
# [timestamp, open, high, low, close, volume]（见各交易所的 `parse_ohlcv()` 实现）。
OHLCV_COLUMNS = ["timestamp", "open", "high", "low", "close", "vol"]

# 同一个 symbol 连续失败多少次后放弃，避免网络异常时无限重试。
MAX_RETRIES = 3

# 单个 symbol 最多发起多少轮分页请求，防止游标推进不了时死循环。
MAX_KLINE_PAGES = 1000
MAX_TRADE_PAGES = 10000


class DataLoadError(RuntimeError):
    """交易所数据多次请求失败。"""


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
        # symbol/pbr 必须按位置传：`f(symbol=symbol, pbr=pbr, *args)` 会先把 *args
        # 绑到形参 symbol/pbr 上，只要 args 非空就直接 TypeError。
        self._load_symbol(symbol, pbr, *args, **kwargs)
        # 收尾必须强制 flush：write_data 默认攒够 10000 条才落盘，而单个 symbol
        # 一天的数据通常远不到这个量，原先直接 _close() 会把整批数据连同文件句柄
        # 一起丢掉，只留下一个空的 CSV（只有表头）。
        self.write_data([], cache=False)
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
        # 不能走 `orjson.loads(df.to_json(orient="records"))`：pandas 的 to_json
        # 默认 double_precision=10，会把价格/成交量静默截断成 10 位小数，
        # 像 SHIB/PEPE 这类单价在 1e-8 量级的币种会直接丢掉有效数字。
        self._write(df.to_dict(orient="records"))
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
        # csv 模块要求以 newline="" 打开，否则由它自己写出的 \r\n 会被文本层
        # 再翻译一次，字段里本身带换行时还会产生无法解析的记录。
        self.csv_file = open(self.csv_path, mode="w", newline="", encoding="utf-8")
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
        # 不能写成 `super().__init__(fieldnames=[...], *args, **kwargs)`：
        # 关键字实参后再展开位置实参（ruff B026）可读性差，且位置实参仍会先于
        # fieldnames 绑定形参，容易出现「多次赋值」的隐蔽错误。
        kwargs.setdefault("fieldnames", ["symbol", *OHLCV_COLUMNS])
        super().__init__(*args, **kwargs)
        self.timeframe = timeframe

    def _load_symbol(self, symbol: str, pbr=None, *args, **kwargs) -> None:
        unix_temp = self.unix_start
        retries = 0
        completed = False
        for _ in range(MAX_KLINE_PAGES):
            if unix_temp >= self.unix_end:
                completed = True
                break
            try:
                result = self.exchange.fetch_ohlcv(
                    symbol, self.timeframe, unix_temp, limit=500
                )
                result = self.exchange.sort_by(result, 0)
                if len(result) == 0:
                    completed = True
                    break
                # since 是闭区间：下一轮必须从最后一根 K 线之后开始，否则这根会被
                # 重复写入；只返回一根时游标更会原地踏步，白跑满 MAX_KLINE_PAGES 轮。
                unix_temp = result[-1][0] + 1
                df = pd.DataFrame(result, columns=OHLCV_COLUMNS)
                df["symbol"] = symbol
                self.write_data(df.to_dict(orient="records"))
                # time.sleep(int(self.exchange.rateLimit / 1000))
            except Exception as e:
                retries += 1
                logger.error(
                    f"拉取K线失败 exchange={self.exchange.id} symbol={symbol} "
                    f"timeframe={self.timeframe} since={unix_temp}: {e}"
                )
                if retries >= MAX_RETRIES:
                    raise DataLoadError(
                        f"exchange={self.exchange.id} symbol={symbol} since={unix_temp}"
                    ) from e
                self.exchange.sleep(1000)
            else:
                retries = 0
        if not completed:
            # 翻页上限兜底：静默截断会产出不完整的当日数据，必须显式告警。
            logger.warning(
                f"K线分页达到上限 {MAX_KLINE_PAGES} 仍未覆盖整个时间区间，数据可能不完整："
                f"exchange={self.exchange.id} symbol={symbol} "
                f"timeframe={self.timeframe} since={unix_temp} end={self.unix_end}"
            )


class TradeLoader(CCXTBaseLoader):
    """逐笔成交数据加载器。"""

    def __init__(self, *args, **kwargs) -> None:
        kwargs.setdefault(
            "fieldnames", ["symbol", "id", "timestamp", "side", "price", "amount"]
        )
        super().__init__(*args, **kwargs)

    def _load_symbol(self, symbol: str, pbr=None, *args, **kwargs) -> None:
        unix_temp = self.unix_start
        previous_trade_id = None
        retries = 0
        completed = False
        for _ in range(MAX_TRADE_PAGES):
            # pbr 是可选参数：`load_symbol("BTC/USDT")` 不带进度条时它就是 None，
            # 原先无条件 .set_description() 必然 AttributeError。
            if pbr is not None:
                pbr.set_description(f"{symbol}-{unix_temp}")
            if unix_temp >= self.unix_end:
                completed = True
                break
            try:
                trades = self.exchange.fetch_trades(symbol, unix_temp, limit=1000)
                # 请求成功就把连续失败计数清零；不能放到 try/else 里，因为下面
                # 两个 `continue` 分支会跳过 else 子句（Python 语言参考明确规定）。
                retries = 0
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
                retries += 1
                logger.error(
                    f"拉取成交记录失败 exchange={self.exchange.id} symbol={symbol} "
                    f"since={unix_temp}: {e}"
                )
                # 与 KlineLoder 一致地设重试上限：交易所持续不可用时原先会一直
                # sleep-retry 到跑满 MAX_TRADE_PAGES 轮（最坏近 3 小时）才罢休。
                if retries >= MAX_RETRIES:
                    raise DataLoadError(
                        f"exchange={self.exchange.id} symbol={symbol} since={unix_temp}"
                    ) from e
                self.exchange.sleep(1000)
        if not completed:
            logger.warning(
                f"成交记录分页达到上限 {MAX_TRADE_PAGES} 仍未覆盖整个时间区间，数据可能不完整："
                f"exchange={self.exchange.id} symbol={symbol} "
                f"since={unix_temp} end={self.unix_end}"
            )
