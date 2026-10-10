"""轻量冒烟测试（smoke tests）。

目标：验证 funcoin 包的核心模块可以正常导入、核心公开类/函数可以在不触发
真实网络请求 / 交易所 API 调用 / 真实凭据的前提下完成构造与基本调用。

不追求覆盖率、不做穷尽式单元测试，仅做“装完包能不能正常用”的兜底检查。
"""

import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import ccxt
import pytest

# ---------------------------------------------------------------------------
# 顶层包 & 子模块导入
# ---------------------------------------------------------------------------


def test_import_top_level_package():
    import funcoin  # noqa: F401


def test_import_coins_subpackages():
    import funcoin.coins
    import funcoin.coins.base
    import funcoin.coins.table
    import funcoin.coins.task  # noqa: F401


def test_import_coins_base_loader():
    import funcoin.coins.base.loader as loader

    assert hasattr(loader, "BaseLoader")
    assert hasattr(loader, "CSVLoader")
    assert hasattr(loader, "CCXTBaseLoader")
    assert hasattr(loader, "KlineLoder")
    assert hasattr(loader, "TradeLoader")


def test_import_coins_table_load():
    import funcoin.coins.table.load as load

    assert hasattr(load, "FileProperty")
    assert hasattr(load, "LoadTask")


def test_import_coins_task_download():
    import funcoin.coins.task.download as download

    assert hasattr(download, "download_daily")


def test_import_server_run():
    """farfarfun/todo-list#157: funcoin.server.run used to import
    `from funserver.base import BaseServer, server_parser`, but that module
    was renamed/moved to `funserver.servers.base` upstream, and
    `server_parser()` itself changed from returning an argparse
    (parser, subparsers) pair to a single Typer app. Both the import path
    and the call site have been updated to match."""
    import funcoin.server.run as run

    assert hasattr(run, "FunCoin")
    assert hasattr(run, "funcoin")


def test_import_server_download():
    import funcoin.server.download as download

    assert hasattr(download, "FunCoinDownload")
    assert hasattr(download, "funcoin_download")


# ---------------------------------------------------------------------------
# funcoin.coins.base.loader
# ---------------------------------------------------------------------------


def test_base_loader_construct_and_noop_load():
    from funcoin.coins.base.loader import BaseLoader

    loader = BaseLoader(unix_start=0, unix_end=1000)
    assert loader.unix_start == 0
    assert loader.unix_end == 1000
    assert loader.cache_data == []

    # 基类的各 hook 都是空实现，调用不应报错、不应发起任何网络/文件操作
    loader.load_symbols()
    loader.load_symbol("BTC/USDT")


def test_csv_loader_construct_and_write(tmp_path):
    from funcoin.coins.base.loader import CSVLoader

    csv_path = os.path.join(str(tmp_path), "test.csv")
    # write_data() 内部按 self.unix_start/unix_end 过滤 "timestamp" 列，
    # 因此测试数据需要携带 timestamp 字段。
    loader = CSVLoader(
        csv_path=csv_path,
        fieldnames=["symbol", "timestamp", "price"],
        unix_start=0,
        unix_end=1000,
    )
    try:
        assert os.path.exists(csv_path)
        loader.write_data(
            [{"symbol": "BTC", "timestamp": 500, "price": 1}], cache=False
        )
    finally:
        loader._close()

    with open(csv_path) as f:
        content = f.read()
    assert "symbol" in content


def test_ccxt_base_loader_construct_with_mocked_exchange(tmp_path):
    """CCXTBaseLoader.__init__ 会调用 exchange.load_markets()，
    用 MagicMock 替身避免真实网络请求。"""
    from funcoin.coins.base.loader import CCXTBaseLoader

    csv_path = os.path.join(str(tmp_path), "test.csv")
    exchange = MagicMock()

    loader = CCXTBaseLoader(
        exchange=exchange,
        csv_path=csv_path,
        fieldnames=["a"],
        unix_start=0,
        unix_end=1000,
    )
    try:
        exchange.load_markets.assert_called_once()
    finally:
        loader._close()


def test_kline_loader_construct_with_mocked_exchange(tmp_path):
    from funcoin.coins.base.loader import KlineLoder

    csv_path = os.path.join(str(tmp_path), "kline.csv")
    exchange = MagicMock()

    loader = KlineLoder(exchange, csv_path=csv_path, unix_start=0, unix_end=1000)
    try:
        assert loader.timeframe == "1m"
    finally:
        loader._close()


def test_trade_loader_construct_with_mocked_exchange(tmp_path):
    from funcoin.coins.base.loader import TradeLoader

    csv_path = os.path.join(str(tmp_path), "trade.csv")
    exchange = MagicMock()

    loader = TradeLoader(exchange, csv_path=csv_path, unix_start=0, unix_end=1000)
    try:
        assert loader.exchange is exchange
    finally:
        loader._close()


def test_kline_loader_load_symbol_success_path(tmp_path):
    """成功路径：fetch_ohlcv 先返回一批数据，随后返回空列表结束抓取。"""
    from funcoin.coins.base.loader import KlineLoder

    csv_path = os.path.join(str(tmp_path), "kline.csv")
    exchange = MagicMock()
    exchange.fetch_ohlcv.side_effect = [[[1000, 1, 2, 3, 4, 5]], []]
    exchange.sort_by.side_effect = lambda data, key: data

    loader = KlineLoder(exchange, csv_path=csv_path, unix_start=0, unix_end=100000)
    try:
        loader._load_symbol("BTC/USDT")
        loader.write_data([], cache=False)  # flush 剩余缓存
    finally:
        loader._close()

    assert exchange.fetch_ohlcv.call_count == 2
    exchange.sleep.assert_not_called()
    with open(csv_path) as f:
        content = f.read()
    assert "BTC/USDT" in content


def test_kline_loader_maps_ccxt_ohlcv_columns_in_order(tmp_path):
    """ccxt 的 fetch_ohlcv 固定返回 [timestamp, open, high, low, close, volume]。

    原实现把列名写成 `["timestamp","open","close","low","high","vol"]`，
    等于把 high 当成 close、close 当成 high 落盘——所有历史 K 线的收盘价都是错的。
    """
    from funcoin.coins.base.loader import OHLCV_COLUMNS, KlineLoder

    assert OHLCV_COLUMNS == ["timestamp", "open", "high", "low", "close", "vol"]

    csv_path = os.path.join(str(tmp_path), "kline.csv")
    exchange = MagicMock()
    # open=10, high=30, low=5, close=20, volume=99
    exchange.fetch_ohlcv.side_effect = [[[1000, 10.0, 30.0, 5.0, 20.0, 99.0]], []]
    exchange.sort_by.side_effect = lambda data, key: data

    written: list[dict] = []
    loader = KlineLoder(exchange, csv_path=csv_path, unix_start=0, unix_end=100000)
    loader._write = written.extend
    try:
        loader._load_symbol("BTC/USDT")
        loader.write_data([], cache=False)
    finally:
        loader._close()

    assert written == [
        {
            "timestamp": 1000,
            "open": 10.0,
            "high": 30.0,
            "low": 5.0,
            "close": 20.0,
            "vol": 99.0,
            "symbol": "BTC/USDT",
        }
    ]


def test_write_data_preserves_full_float_precision(tmp_path):
    """低价币的价格不能被截断。

    原实现走 `orjson.loads(df.to_json(orient="records"))`，而 pandas 的 to_json
    默认 double_precision=10，8.123456789e-06 会被压成 8.1235e-06（丢 5 位有效数字）。
    """
    from funcoin.coins.base.loader import CSVLoader

    price = 8.123456789e-06
    csv_path = os.path.join(str(tmp_path), "precision.csv")
    written: list[dict] = []
    loader = CSVLoader(
        csv_path=csv_path,
        fieldnames=["symbol", "timestamp", "price"],
        unix_start=0,
        unix_end=1000,
    )
    loader._write = written.extend
    try:
        loader.write_data(
            [{"symbol": "SHIB/USDT", "timestamp": 500, "price": price}], cache=False
        )
    finally:
        loader._close()

    assert written[0]["price"] == price


def test_kline_loader_advances_cursor_past_last_candle(tmp_path):
    """`since` 是闭区间：游标必须跨过最后一根 K 线。

    原实现 `unix_temp = result[-1][0]`，下一轮会把同一根 K 线再拉一次并重复写入；
    交易所只返回一根时游标更是原地踏步，白跑满 1000 轮分页。
    """
    from funcoin.coins.base.loader import KlineLoder

    csv_path = os.path.join(str(tmp_path), "kline.csv")
    exchange = MagicMock()
    exchange.fetch_ohlcv.side_effect = [[[1000, 1, 2, 3, 4, 5]], []]
    exchange.sort_by.side_effect = lambda data, key: data

    loader = KlineLoder(exchange, csv_path=csv_path, unix_start=0, unix_end=100000)
    try:
        loader._load_symbol("BTC/USDT")
    finally:
        loader._close()

    second_call = exchange.fetch_ohlcv.call_args_list[1]
    assert second_call.args[2] == 1001


def test_kline_loader_raises_after_max_retries(tmp_path):
    """连续失败达到上限时抛 DataLoadError，而不是无声地跑完分页上限。"""
    from funcoin.coins.base.loader import DataLoadError, KlineLoder

    csv_path = os.path.join(str(tmp_path), "kline.csv")
    exchange = MagicMock()
    exchange.id = "binance"
    exchange.fetch_ohlcv.side_effect = Exception("boom")

    loader = KlineLoder(exchange, csv_path=csv_path, unix_start=0, unix_end=100000)
    try:
        with pytest.raises(DataLoadError):
            loader._load_symbol("BTC/USDT")
    finally:
        loader._close()

    assert exchange.fetch_ohlcv.call_count == 3


def test_trade_loader_load_symbol_without_progress_bar(tmp_path):
    """pbr 是可选参数：不传进度条时不应该 AttributeError。

    `load_symbol("BTC/USDT")` 这条公开路径默认 pbr=None，原实现无条件调用
    `pbr.set_description(...)`，必然在第一轮就炸。
    """
    from funcoin.coins.base.loader import TradeLoader

    csv_path = os.path.join(str(tmp_path), "trade.csv")
    exchange = MagicMock()
    exchange.fetch_trades.side_effect = [[], []]

    loader = TradeLoader(
        exchange, csv_path=csv_path, unix_start=0, unix_end=3600 * 1000
    )
    try:
        loader._load_symbol("BTC/USDT")
    finally:
        loader._close()

    assert exchange.fetch_trades.call_count == 1


def test_trade_loader_raises_after_max_retries(tmp_path):
    """TradeLoader 也要有重试上限，不能一直 sleep-retry 到跑满 10000 轮。"""
    from funcoin.coins.base.loader import DataLoadError, TradeLoader

    csv_path = os.path.join(str(tmp_path), "trade.csv")
    exchange = MagicMock()
    exchange.id = "binance"
    exchange.fetch_trades.side_effect = ccxt.NetworkError("boom")

    loader = TradeLoader(
        exchange, csv_path=csv_path, unix_start=0, unix_end=3600 * 1000
    )
    try:
        with pytest.raises(DataLoadError):
            loader._load_symbol("BTC/USDT")
    finally:
        loader._close()

    assert exchange.fetch_trades.call_count == 3


def test_load_symbol_forwards_extra_positional_args(tmp_path):
    """`load_symbol()` 转发额外位置参数时不能 TypeError。

    原实现写成 `self._load_symbol(symbol=symbol, pbr=pbr, *args, **kwargs)`，
    Python 会先把 *args 绑到形参 symbol/pbr 上，只要 args 非空就「多次赋值」报错。
    """
    from funcoin.coins.base.loader import BaseLoader

    seen = {}

    class _Loader(BaseLoader):
        def _load_symbol(self, symbol, pbr=None, *args, **kwargs):
            seen["symbol"] = symbol
            seen["args"] = args

    loader = _Loader(unix_start=0, unix_end=1000)
    loader.load_symbol("BTC/USDT", None, "extra")

    assert seen == {"symbol": "BTC/USDT", "args": ("extra",)}


def test_load_symbol_flushes_cache_before_closing_file(tmp_path):
    """`load_symbol()` 收尾必须强制 flush，否则不足 10000 条的数据会被整批丢掉。

    `write_data(cache=True)` 攒够 10000 条才落盘，而单 symbol 单日的数据远不到这个量；
    原实现直接 `_close()` 关文件，产出的 CSV 只有表头。这也是 README 最小示例的回归点。
    """
    import csv as csv_module

    from funcoin.coins.base.loader import KlineLoder

    csv_path = os.path.join(str(tmp_path), "kline.csv")
    exchange = MagicMock()
    exchange.fetch_ohlcv.side_effect = [
        [[1700000000000, 10.0, 30.0, 5.0, 20.0, 99.0]],
        [],
    ]
    exchange.sort_by.side_effect = lambda data, key: data

    loader = KlineLoder(
        exchange,
        csv_path=csv_path,
        unix_start=1700000000000,
        unix_end=1700000600000,
    )
    loader.load_symbol("BTC/USDT")

    with open(csv_path, encoding="utf-8") as fp:
        rows = list(csv_module.DictReader(fp))

    assert len(rows) == 1
    assert rows[0]["symbol"] == "BTC/USDT"
    assert rows[0]["close"] == "20.0"
    assert rows[0]["vol"] == "99.0"


def test_kline_loader_load_symbol_empty_result_boundary(tmp_path):
    """边界路径：unix_start 已经 >= unix_end，直接跳过，不发起任何请求。"""
    from funcoin.coins.base.loader import KlineLoder

    csv_path = os.path.join(str(tmp_path), "kline.csv")
    exchange = MagicMock()

    loader = KlineLoder(exchange, csv_path=csv_path, unix_start=1000, unix_end=1000)
    try:
        loader._load_symbol("BTC/USDT")
    finally:
        loader._close()

    exchange.fetch_ohlcv.assert_not_called()


def test_kline_loader_load_symbol_network_error_path(tmp_path):
    """异常路径：请求出错时记录带上下文的日志并 sleep，随后恢复继续抓取。"""
    from funcoin.coins.base.loader import KlineLoder

    csv_path = os.path.join(str(tmp_path), "kline.csv")
    exchange = MagicMock()
    exchange.id = "binance"
    exchange.fetch_ohlcv.side_effect = [Exception("boom"), []]

    loader = KlineLoder(exchange, csv_path=csv_path, unix_start=0, unix_end=100000)
    try:
        loader._load_symbol("BTC/USDT")
    finally:
        loader._close()

    assert exchange.fetch_ohlcv.call_count == 2
    exchange.sleep.assert_called_once_with(1000)


def test_trade_loader_load_symbol_success_path(tmp_path):
    """成功路径：fetch_trades 返回一笔成交，随后返回空列表推进游标直至越界退出。"""
    from funcoin.coins.base.loader import TradeLoader

    one_hour = 3600 * 1000
    csv_path = os.path.join(str(tmp_path), "trade.csv")
    exchange = MagicMock()
    exchange.fetch_trades.side_effect = [
        [
            {
                "symbol": "BTC/USDT",
                "id": "trade-1",
                "timestamp": 1000,
                "side": "buy",
                "price": 100.0,
                "amount": 1,
            }
        ],
        [],
    ]

    loader = TradeLoader(exchange, csv_path=csv_path, unix_start=0, unix_end=one_hour)
    pbr = MagicMock()
    try:
        loader._load_symbol("BTC/USDT", pbr=pbr)
        loader.write_data([], cache=False)
    finally:
        loader._close()

    assert exchange.fetch_trades.call_count == 2
    exchange.sleep.assert_not_called()
    with open(csv_path) as f:
        content = f.read()
    assert "trade-1" in content


def test_trade_loader_load_symbol_network_error_path(tmp_path):
    """异常路径：ccxt.NetworkError 时记录带上下文的日志并 sleep，之后越界正常退出。"""
    from funcoin.coins.base.loader import TradeLoader

    one_hour = 3600 * 1000
    csv_path = os.path.join(str(tmp_path), "trade.csv")
    exchange = MagicMock()
    exchange.id = "binance"
    exchange.fetch_trades.side_effect = [ccxt.NetworkError("boom"), []]

    loader = TradeLoader(exchange, csv_path=csv_path, unix_start=0, unix_end=one_hour)
    pbr = MagicMock()
    try:
        loader._load_symbol("BTC/USDT", pbr=pbr)
    finally:
        loader._close()

    assert exchange.fetch_trades.call_count == 2
    exchange.sleep.assert_called_once_with(1000)


# ---------------------------------------------------------------------------
# funcoin.coins.table.load
# ---------------------------------------------------------------------------


def test_file_property_daily_paths():
    from funcoin.coins.table.load import FileProperty

    fp = FileProperty("binance", data_type="kline", timeframe="1m").daily("20260101")

    assert fp.partition == "202601"
    assert fp.filename_prefix == "binance_kline_daily_1m-20260101"
    assert fp.file_path_csv == "binance_kline_daily_1m-20260101.csv"
    assert fp.file_path_tar == "binance_kline_daily_1m-20260101.tar"


def test_load_task_construct():
    from funcoin.coins.table.load import LoadTask

    table = MagicMock()
    exchange = MagicMock()
    task = LoadTask(table=table, exchange=exchange)

    assert task.table is table
    assert task.exchange is exchange


def test_load_task_download_success_uploads_and_cleans_up(tmp_path, monkeypatch):
    """成功路径：压缩、上传后本地临时 csv/tar 文件都应被清理。"""
    from funcoin.coins.table.load import FileProperty, LoadTask

    monkeypatch.chdir(tmp_path)
    file_pro = FileProperty("binance", data_type="kline", timeframe="1m").daily(
        "20260101"
    )
    with open(file_pro.file_path_csv, "w") as f:
        f.write("symbol,timestamp\nBTC/USDT,1000\n")

    loader = MagicMock()
    table = MagicMock()
    task = LoadTask(table=table, exchange=MagicMock())

    result = task.download(loader, file_pro)

    assert result is True
    loader.load_symbols.assert_called_once()
    table.upload.assert_called_once_with(
        file=file_pro.file_path_tar, partition=file_pro.partition, overwrite=True
    )
    assert not os.path.exists(file_pro.file_path_csv)
    assert not os.path.exists(file_pro.file_path_tar)


def test_load_task_download_cleans_up_on_upload_failure(tmp_path, monkeypatch):
    """失败清理路径：上传抛异常时，本地临时文件仍要被清理，且异常继续向上传播。"""
    from funcoin.coins.table.load import FileProperty, LoadTask

    monkeypatch.chdir(tmp_path)
    file_pro = FileProperty("binance", data_type="kline", timeframe="1m").daily(
        "20260102"
    )
    with open(file_pro.file_path_csv, "w") as f:
        f.write("symbol,timestamp\nBTC/USDT,1000\n")

    loader = MagicMock()
    table = MagicMock()
    table.upload.side_effect = RuntimeError("upload failed")
    task = LoadTask(table=table, exchange=MagicMock())

    with pytest.raises(RuntimeError, match="upload failed"):
        task.download(loader, file_pro)

    assert not os.path.exists(file_pro.file_path_csv)
    assert not os.path.exists(file_pro.file_path_tar)


def test_load_task_run_boundary_zero_days_is_noop():
    """边界路径：days=0 时不应下载任何一天的数据。"""
    from funcoin.coins.table.load import LoadTask

    table = MagicMock()
    table.partition_meta.return_value = []
    exchange = MagicMock()
    exchange.name = "Binance"
    task = LoadTask(table=table, exchange=exchange)
    task.download_kline = MagicMock()

    task.run(days=0)

    task.download_kline.assert_not_called()


def test_load_task_run_default_days_matches_shared_default():
    """直接调用 LoadTask 时也必须使用 CLI/调度器相同的回补天数。"""
    import inspect

    from funcoin.coins.table.load import LoadTask
    from funcoin.coins.task.download import DEFAULT_DAYS

    assert inspect.signature(LoadTask.run).parameters["days"].default == DEFAULT_DAYS


def test_load_task_run_skips_existing_partition():
    """已存在的分区应跳过下载，避免重复拉取。"""

    from funcoin.coins.table.load import FileProperty, LoadTask

    table = MagicMock()
    exchange = MagicMock()
    exchange.name = "Binance"
    task = LoadTask(table=table, exchange=exchange)
    task.download_kline = MagicMock()

    yesterday = datetime.now(UTC) - timedelta(days=1)
    existing_tar = (
        FileProperty("binance").daily(yesterday.strftime("%Y%m%d")).file_path_tar
    )
    table.partition_meta.return_value = [{"name": existing_tar}]

    task.run(days=1)

    task.download_kline.assert_not_called()


def test_load_task_run_starts_from_yesterday_not_the_day_before():
    """`days=1` 必须处理**昨天**。

    原实现在循环体里先 `start_day += timedelta(days=-1)` 再使用，导致 days=1
    实际下载的是前天、最近的一天永远补不上（与 docstring 差一天）。
    """
    from funcoin.coins.table.load import FileProperty, LoadTask

    table = MagicMock()
    table.partition_meta.return_value = []
    exchange = MagicMock()
    exchange.name = "Binance"
    task = LoadTask(table=table, exchange=exchange)
    task.download_kline = MagicMock()

    task.run(days=1)

    task.download_kline.assert_called_once()
    (file_pro,) = task.download_kline.call_args.args
    yesterday = datetime.now(UTC) - timedelta(days=1)
    assert (
        file_pro.file_path_tar
        == FileProperty("binance").daily(yesterday.strftime("%Y%m%d")).file_path_tar
    )


def test_file_property_daily_boundaries_are_utc():
    """按天切分必须以 UTC 为准，否则换个时区的机器跑出的同名文件内容不一致。"""
    from funcoin.coins.table.load import FileProperty

    fp = FileProperty("binance").daily("20260101")

    assert fp.start_date.tzinfo is not None
    assert fp.start_date == datetime(2026, 1, 1, tzinfo=UTC)
    assert fp.end_date == datetime(2026, 1, 2, tzinfo=UTC)
    # 1767225600000 = 2026-01-01T00:00:00Z，loader 的 unix_start 就是由它换算而来。
    assert int(fp.start_date.timestamp() * 1000) == 1767225600000


# ---------------------------------------------------------------------------
# funcoin.coins.task.download
# ---------------------------------------------------------------------------


def test_download_daily_wires_components_without_network(monkeypatch):
    """用 mock 替身验证 download_daily() 的编排逻辑，不发起任何真实网络请求
    /交易所调用/云存储登录。"""
    import funcoin.coins.task.download as download_mod

    fake_exchange = MagicMock()
    fake_drive = MagicMock()
    fake_table = MagicMock()
    fake_task = MagicMock()

    monkeypatch.setattr(
        download_mod.ccxt, "binance", MagicMock(return_value=fake_exchange)
    )
    monkeypatch.setattr(download_mod, "read_secret", MagicMock(return_value="fake"))
    monkeypatch.setattr(download_mod, "OSSDrive", MagicMock(return_value=fake_drive))
    monkeypatch.setattr(download_mod, "DriveTable", MagicMock(return_value=fake_table))
    fake_load_task_cls = MagicMock(return_value=fake_task)
    monkeypatch.setattr(download_mod, "LoadTask", fake_load_task_cls)

    download_mod.download_daily(days=5)

    fake_drive.login.assert_called_once()
    fake_table.update_partition_meta.assert_called_once()
    fake_load_task_cls.assert_called_once_with(table=fake_table, exchange=fake_exchange)
    fake_task.run.assert_called_once_with(days=5)


# ---------------------------------------------------------------------------
# funcoin.server.scheduler —— 常驻调度循环
# ---------------------------------------------------------------------------


def test_run_download_loop_once_runs_exactly_one_round():
    from funcoin.server.scheduler import run_download_loop

    task = MagicMock()
    slept: list[float] = []

    rounds = run_download_loop(
        days=3, interval=123, once=True, task=task, sleep=slept.append
    )

    assert rounds == 1
    task.assert_called_once_with(days=3)
    assert slept == []  # once 模式不进入休眠


def test_run_download_loop_once_propagates_failure():
    """一次性模式必须把异常抛出去，CLI/CI 才能拿到非零退出码。"""
    from funcoin.server.scheduler import run_download_loop

    task = MagicMock(side_effect=RuntimeError("boom"))

    with pytest.raises(RuntimeError, match="boom"):
        run_download_loop(
            days=1, interval=1, once=True, task=task, sleep=lambda _: None
        )


def test_run_download_loop_keeps_running_and_sleeps_between_rounds():
    """常驻模式：跑完一轮休眠 interval 秒后继续下一轮，单轮失败不退出服务。"""
    from funcoin.server.scheduler import run_download_loop

    calls = {"n": 0}
    slept: list[float] = []

    def task(days):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("第一轮失败，服务不应因此退出")

    def sleep(seconds):
        slept.append(seconds)
        # 第二轮结束后的休眠里模拟收到 SIGTERM，让循环优雅退出。
        if calls["n"] >= 2:
            raise KeyboardInterrupt

    rounds = run_download_loop(days=2, interval=3, once=False, task=task, sleep=sleep)

    assert rounds == 2
    # 休眠被切成 1 秒一片，保证 SIGTERM 之后不用等满整个 interval。
    assert slept[:3] == [1.0, 1.0, 1.0]


def test_run_download_loop_reads_env_defaults(monkeypatch):
    from funcoin.coins.task.download import DEFAULT_DAYS
    from funcoin.server import scheduler

    monkeypatch.delenv("FUNCOIN_DOWNLOAD_DAYS", raising=False)
    monkeypatch.delenv("FUNCOIN_DOWNLOAD_INTERVAL", raising=False)
    monkeypatch.delenv("FUNCOIN_RUN_ONCE", raising=False)
    assert scheduler.resolve_days() == DEFAULT_DAYS
    assert scheduler.resolve_interval() == scheduler.DEFAULT_INTERVAL_SECONDS
    assert scheduler.resolve_once() is False

    monkeypatch.setenv("FUNCOIN_DOWNLOAD_DAYS", "7")
    monkeypatch.setenv("FUNCOIN_DOWNLOAD_INTERVAL", "3600")
    monkeypatch.setenv("FUNCOIN_RUN_ONCE", "1")
    assert scheduler.resolve_days() == 7
    assert scheduler.resolve_interval() == 3600
    assert scheduler.resolve_once() is True

    # 非法值回落到默认值而不是崩掉服务
    monkeypatch.setenv("FUNCOIN_DOWNLOAD_INTERVAL", "not-a-number")
    assert scheduler.resolve_interval() == scheduler.DEFAULT_INTERVAL_SECONDS
    # 低于下限按下限处理，避免 interval=0 时空转
    monkeypatch.setenv("FUNCOIN_DOWNLOAD_INTERVAL", "0")
    assert scheduler.resolve_interval() == scheduler.MIN_RETRY_SECONDS


def test_service_entrypoints_are_long_running():
    """README 把 `start/run` 描述成长期运行服务，两个服务入口必须真的常驻。

    `BaseServer._start()` 用 `nohup ... &` 把 run() 丢到后台，只调一次
    download_daily() 就返回的话进程会立刻退出，status 永远显示未运行。
    """
    import funcoin.server.download as download_mod
    import funcoin.server.run as run_mod

    def _check(module, cls_name, monkeypatch_target):
        called = []
        original = module.run_download_loop
        module.run_download_loop = lambda *a, **k: called.append(1)
        try:
            # object.__new__ 跳过 BaseServer.__init__（它会在 ~/.cache 下建目录）。
            server = object.__new__(monkeypatch_target)
            server.run()
        finally:
            module.run_download_loop = original
        assert called == [1], f"{cls_name}.run() 没有进入常驻循环"

    _check(download_mod, "FunCoinDownload", download_mod.FunCoinDownload)
    _check(run_mod, "FunCoin", run_mod.FunCoin)


def test_cli_download_default_days_matches_library_default():
    """CLI 的 --days 默认值必须和 download_daily() 一致，不能 README 写一套、代码跑另一套。"""
    import inspect

    from funcoin.coins.task.download import DEFAULT_DAYS, download_daily

    assert inspect.signature(download_daily).parameters["days"].default == DEFAULT_DAYS


# ---------------------------------------------------------------------------
# CLI entry points ([project.scripts])
# ---------------------------------------------------------------------------


def test_cli_funcoin_help():
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from funcoin.server.run import funcoin; import sys; sys.argv=['funcoin', '--help']; funcoin()",
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr


def test_cli_funcoin_download_help():
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from funcoin.server.download import funcoin_download; import sys; "
            "sys.argv=['funcoin-download', '--help']; funcoin_download()",
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr


def test_cli_funcoin_download_subcommand_help():
    """funcoin's Typer app gets an extra `download` command grafted on
    (funcoin.coins.task.download.download_daily, exposed with a --days
    option) on top of the base server_parser() commands."""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from funcoin.server.run import funcoin; import sys; "
            "sys.argv=['funcoin', 'download', '--help']; funcoin()",
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert "--days" in result.stdout


def test_setup_script_uses_installed_cli_without_environment_argument(tmp_path):
    """生命周期只转发给已安装 CLI，dev/prod 只属于安装阶段。"""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "python3").write_text(
        "#!/bin/sh\n"
        'if [ "${1:-}" = -I ]; then\n'
        '    if [ "${2:-}" = - ]; then\n'
        "        cat >/dev/null\n"
        "    else\n"
        "        echo 1.0.57\n"
        "    fi\n"
        "else\n"
        "    printf 'python:%s\\n' \"$*\"\n"
        "fi\n",
        encoding="utf-8",
    )
    (bin_dir / "funcoin-download").write_text(
        "#!/bin/sh\nprintf 'cli:%s\\n' \"$*\"\n", encoding="utf-8"
    )
    (bin_dir / "funbuild").write_text(
        "#!/bin/sh\nprintf 'funbuild:%s\\n' \"$*\"\n", encoding="utf-8"
    )
    for executable in bin_dir.iterdir():
        executable.chmod(0o755)

    script = os.path.join(os.path.dirname(__file__), "..", "scripts", "setup.sh")
    env = {**os.environ, "PATH": f"{bin_dir}:/usr/bin:/bin"}

    run = subprocess.run(
        ["bash", script, "run"], capture_output=True, text=True, env=env, timeout=10
    )
    assert run.returncode == 0, run.stderr
    assert run.stdout.strip() == "cli:run"

    old_model = subprocess.run(
        ["bash", script, "run", "dev"], capture_output=True, text=True, env=env
    )
    assert old_model.returncode != 0
    assert "install-dev" in old_model.stderr

    install_prod = subprocess.run(
        ["bash", script, "install-prod", "1.2.3"],
        capture_output=True,
        text=True,
        env=env,
    )
    assert install_prod.returncode == 0, install_prod.stderr
    assert install_prod.stdout.strip() == "python:-m pip install funcoin==1.2.3"

    install_dev = subprocess.run(
        ["bash", script, "install-dev"], capture_output=True, text=True, env=env
    )
    assert install_dev.returncode == 0, install_dev.stderr
    assert install_dev.stdout.strip() == "funbuild:install"

    publish = subprocess.run(
        ["bash", script, "publish"], capture_output=True, text=True, env=env
    )
    assert publish.returncode == 0, publish.stderr
    assert publish.stdout.strip() == "funbuild:build"
