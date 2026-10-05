"""下载服务的调度循环。

`funserver` 的 `BaseServer._start()` 会用 `nohup ... &` 把 `run()` 丢到后台，
因此 `run()` 必须自己持续运行；只调用一次下载任务就返回的话，进程会立刻退出，
`status` 永远显示「未运行」。本模块提供一个可中断、可注入、可测试的调度循环。
"""

import os
import signal
import time
from collections.abc import Callable

from farlog import getLogger

from funcoin.coins.task.download import DEFAULT_DAYS, download_daily

logger = getLogger("funcoin")

# 两轮下载之间的默认间隔：行情按天归档，一天跑一次即可。
DEFAULT_INTERVAL_SECONDS = 24 * 60 * 60

# 单轮失败后的最短重试间隔，避免交易所持续不可用时空转刷屏。
MIN_RETRY_SECONDS = 60

_TRUTHY = {"1", "true", "yes", "on"}


def _env_flag(name: str) -> bool:
    """读取布尔型环境变量，未设置时返回 False。"""
    return os.environ.get(name, "").strip().lower() in _TRUTHY


def _env_number(name: str, default: float, minimum: float) -> float:
    """读取数值型环境变量，非法或过小时回落到默认值并告警。"""
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = float(raw)
    except ValueError:
        logger.warning(f"环境变量 {name}={raw!r} 不是合法数字，回落到默认值 {default}")
        return default
    if value < minimum:
        logger.warning(f"环境变量 {name}={raw!r} 小于下限 {minimum}，按 {minimum} 处理")
        return minimum
    return value


def resolve_days() -> int:
    """解析每轮回补的天数（环境变量 `FUNCOIN_DOWNLOAD_DAYS`）。"""
    return int(_env_number("FUNCOIN_DOWNLOAD_DAYS", DEFAULT_DAYS, 1))


def resolve_interval() -> float:
    """解析两轮之间的间隔秒数（环境变量 `FUNCOIN_DOWNLOAD_INTERVAL`）。"""
    return _env_number(
        "FUNCOIN_DOWNLOAD_INTERVAL", DEFAULT_INTERVAL_SECONDS, MIN_RETRY_SECONDS
    )


def resolve_once() -> bool:
    """是否只跑一轮就退出（环境变量 `FUNCOIN_RUN_ONCE`）。"""
    return _env_flag("FUNCOIN_RUN_ONCE")


class _StopSignal:
    """把 SIGTERM/SIGINT 转成一个可轮询的停止标志，实现优雅退出。

    只在主线程里注册得上；在子线程（或已被宿主框架接管信号时）会抛
    `ValueError`，此时退化成「不处理信号」，循环仍可被 KeyboardInterrupt 打断。
    """

    def __init__(self) -> None:
        self.stopped = False
        self._previous: dict[int, object] = {}

    def __enter__(self) -> "_StopSignal":
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                self._previous[sig] = signal.signal(sig, self._handle)
            except (ValueError, OSError, AttributeError):
                # 非主线程 / 平台不支持该信号，放弃注册但不影响主流程。
                continue
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        for sig, handler in self._previous.items():
            try:
                signal.signal(sig, handler)
            except (ValueError, OSError, TypeError):
                continue
        return False

    def _handle(self, signum, frame) -> None:
        logger.info(f"收到信号 {signum}，当前轮次结束后退出")
        self.stopped = True


def run_download_loop(
    days: int | None = None,
    interval: float | None = None,
    once: bool | None = None,
    task: Callable[..., None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    """持续按固定间隔执行每日行情下载，直到收到停止信号。

    Args:
        days: 每轮回补的天数，缺省时读 `FUNCOIN_DOWNLOAD_DAYS`。
        interval: 两轮之间的间隔秒数，缺省时读 `FUNCOIN_DOWNLOAD_INTERVAL`。
        once: 为 True 时只跑一轮就返回（缺省时读 `FUNCOIN_RUN_ONCE`），
            单轮失败会把异常原样抛出，便于 CI / 人工排查。
        task: 实际执行的下载函数，默认 `download_daily`，测试可注入替身。
        sleep: 休眠函数，测试可注入替身。

    Returns:
        实际执行的轮次数。
    """
    task = task or download_daily
    days = resolve_days() if days is None else int(days)
    interval = resolve_interval() if interval is None else float(interval)
    once = resolve_once() if once is None else bool(once)

    rounds = 0
    with _StopSignal() as stop:
        while True:
            rounds += 1
            logger.info(f"开始第 {rounds} 轮行情下载: days={days}")
            try:
                task(days=days)
            except KeyboardInterrupt:
                logger.info("收到中断，停止下载服务")
                break
            except Exception:
                if once:
                    # 一次性模式下不吞异常：调用方（CLI / CI）需要非零退出码。
                    raise
                # 常驻模式下单轮失败不能让整个服务退出，记日志后等下一轮重试。
                logger.exception(f"第 {rounds} 轮行情下载失败，{interval:.0f} 秒后重试")
            else:
                logger.info(f"第 {rounds} 轮行情下载完成")

            if once or stop.stopped:
                break
            if not _interruptible_sleep(interval, stop, sleep):
                break

    return rounds


def _interruptible_sleep(
    seconds: float, stop: _StopSignal, sleep: Callable[[float], None]
) -> bool:
    """分片休眠，收到停止信号时提前返回 False。

    一次性 `sleep(86400)` 会让 SIGTERM 之后的退出延迟到一整天之后，
    所以切成 1 秒一片轮询停止标志。
    """
    remaining = seconds
    while remaining > 0:
        if stop.stopped:
            return False
        slice_seconds = min(1.0, remaining)
        try:
            sleep(slice_seconds)
        except KeyboardInterrupt:
            return False
        remaining -= slice_seconds
    return not stop.stopped
