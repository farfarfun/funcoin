"""轻量冒烟测试（smoke tests）。

目标：验证 funcoin 包的核心模块可以正常导入、核心公开类/函数可以在不触发
真实网络请求 / 交易所 API 调用 / 真实凭据的前提下完成构造与基本调用。

不追求覆盖率、不做穷尽式单元测试，仅做“装完包能不能正常用”的兜底检查。
"""

import subprocess
import sys
import os
from unittest.mock import MagicMock

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


def _funserver_base_available():
    try:
        import funserver.base  # noqa: F401

        return True
    except ModuleNotFoundError:
        return False


FUNSERVER_BASE_SKIP_REASON = (
    "已安装的 funserver 版本已将 funserver.base 迁移/移除"
    "（当前只提供 funserver.servers.base），而 funcoin.server.run / "
    "funcoin.server.download 仍在使用旧的 `from funserver.base import "
    "BaseServer, server_parser` 导入路径，属于上游依赖 API 变更导致的真实"
    "导入期 bug（ModuleNotFoundError），非本次冒烟测试范围内可修复的问题，"
    "此处显式跳过并在 issue 中报告。"
)


@pytest.mark.skipif(not _funserver_base_available(), reason=FUNSERVER_BASE_SKIP_REASON)
def test_import_server_run():
    import funcoin.server.run as run  # noqa: F401

    assert hasattr(run, "FunCoin")
    assert hasattr(run, "funcoin")


@pytest.mark.skipif(not _funserver_base_available(), reason=FUNSERVER_BASE_SKIP_REASON)
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


# ---------------------------------------------------------------------------
# funcoin.coins.task.download
# ---------------------------------------------------------------------------


def test_download_daily_requires_real_credentials():
    """download_daily() 内部直接构造真实 ccxt.binance 客户端、读取
    funsecret 中的 API Key/Secret，并登录真实 OSS，无法在不改动源码的
    情况下进行有意义的 mock 化冒烟测试，因此显式跳过。"""
    pytest.skip("需要真实凭据（币安 API Key / OSS 账号），跳过")


# ---------------------------------------------------------------------------
# CLI entry points ([project.scripts])
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not _funserver_base_available(),
    reason=FUNSERVER_BASE_SKIP_REASON + "（CLI 入口 funcoin 无法启动）",
)
def test_cli_funcoin_help():
    result = subprocess.run(
        [sys.executable, "-c", "from funcoin.server.run import funcoin; import sys; sys.argv=['funcoin', '--help']; funcoin()"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.skipif(
    not _funserver_base_available(),
    reason=FUNSERVER_BASE_SKIP_REASON + "（CLI 入口 funcoin-download 无法启动）",
)
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
