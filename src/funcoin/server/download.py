from funserver.servers.base import BaseServer, server_parser
from funshell import run_shell_list

from funcoin.coins.task.download import DEFAULT_DAYS, download_daily
from funcoin.server.scheduler import run_download_loop


class FunCoinDownload(BaseServer):
    """每日行情下载服务（常驻进程，按固定间隔循环下载）。"""

    def __init__(self) -> None:
        """初始化服务名称。"""
        super().__init__(server_name="funcoin-download")

    def update(self, args: object | None = None, **kwargs: object) -> None:
        """升级已安装的 funcoin 包。"""
        run_shell_list(["pip install funcoin -U"])

    def run(self, *args: object, **kwargs: object) -> None:
        """常驻运行：按 `FUNCOIN_DOWNLOAD_INTERVAL` 的间隔循环执行每日行情下载。

        `BaseServer._start()` 会用 `nohup ... &` 把本方法丢到后台，因此它必须自己
        持续运行；只跑一次就返回的话进程立刻退出，`status` 永远显示「未运行」。
        需要「跑完一轮就退出」时设环境变量 `FUNCOIN_RUN_ONCE=1`，
        或直接用 `funcoin-download download` 子命令。
        """
        run_download_loop()


def funcoin_download() -> None:
    """启动命令行服务入口。"""
    server = FunCoinDownload()
    app = server_parser(server)

    @app.command()
    def download(days: int = DEFAULT_DAYS):
        """立即执行一次下载后退出（不进入常驻循环）。"""
        download_daily(days=days)

    app()
