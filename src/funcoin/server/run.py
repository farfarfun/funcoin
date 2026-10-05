from funserver.servers.base import BaseServer, server_parser
from funshell import run_shell_list

from funcoin.coins.task.download import DEFAULT_DAYS, download_daily
from funcoin.server.scheduler import run_download_loop


class FunCoin(BaseServer):
    """funcoin 命令行服务。"""

    def __init__(self) -> None:
        """初始化服务名称。"""
        super().__init__(server_name="funcoin")

    def update(self, args: object | None = None, **kwargs: object) -> None:
        """升级已安装的 funcoin 包。"""
        run_shell_list(["pip install funcoin -U"])

    def run(self, *args: object, **kwargs: object) -> None:
        """常驻运行：按固定间隔循环执行每日行情下载。

        与 `funcoin-download` 的服务入口行为一致；只想跑一次用
        `funcoin download`（或设环境变量 `FUNCOIN_RUN_ONCE=1`）。
        """
        run_download_loop()


def funcoin() -> None:
    """启动 funcoin 命令行入口。"""
    server = FunCoin()
    app = server_parser(server)

    @app.command()
    def download(days: int = DEFAULT_DAYS):
        """立即执行一次下载后退出（不进入常驻循环）。"""
        download_daily(days=days)

    app()
