"""轻量冒烟测试（smoke tests）。

目标：验证 funcoin 包的核心模块可以正常导入、核心公开类/函数可以在不触发
真实网络请求 / 交易所 API 调用 / 真实凭据的前提下完成构造与基本调用。

不追求覆盖率、不做穷尽式单元测试，仅做“装完包能不能正常用”的兜底检查。
"""

import subprocess
import sys
import os
from unittest.mock import MagicMock

import ccxt
import pytest


# ---------------------------------------------------------------------------
# 顶层包 & 子模块导入
# ---------------------------------------------------------------------------


def test_import_top_level_package():
    import funcoin  # noqa: F401


def test_import_coins_subpackages():
    import funcoin.coins  # noqa: F401
    import funcoin.coins.base  # noqa: F401
    import funcoin.coins.table  # noqa: F401
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
    import funcoin.server.run as run  # noqa: F401

    assert hasattr(run, "FunCoin")
    assert hasattr(run, "funcoin")


def test_import_server_download():
    import funcoin.server.download as download  # noqa: F401

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
        loader.write_data([{"symbol": "BTC", "timestamp": 500, "price": 1}], cache=False)
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
        exchange=exchange, csv_path=csv_path, fieldnames=["a"], unix_start=0, unix_end=1000
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
    file_pro = FileProperty("binance", data_type="kline", timeframe="1m").daily("20260101")
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
    file_pro = FileProperty("binance", data_type="kline", timeframe="1m").daily("20260102")
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


def test_load_task_run_skips_existing_partition():
    """已存在的分区应跳过下载，避免重复拉取。"""
    from datetime import datetime, timedelta

    from funcoin.coins.table.load import FileProperty, LoadTask

    table = MagicMock()
    exchange = MagicMock()
    exchange.name = "Binance"
    task = LoadTask(table=table, exchange=exchange)
    task.download_kline = MagicMock()

    target_day = datetime.now() - timedelta(days=2)
    existing_tar = (
        FileProperty("binance").daily(target_day.strftime("%Y%m%d")).file_path_tar
    )
    table.partition_meta.return_value = [{"name": existing_tar}]

    task.run(days=1)

    task.download_kline.assert_not_called()


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
# CLI entry points ([project.scripts])
# ---------------------------------------------------------------------------


def test_cli_funcoin_help():
    result = subprocess.run(
        [sys.executable, "-c", "from funcoin.server.run import funcoin; import sys; sys.argv=['funcoin', '--help']; funcoin()"],
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
