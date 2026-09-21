from funserver.servers.base import BaseServer, server_parser
from funshell import run_shell_list

from funcoin.coins.task.download import download_daily


class FunCoin(BaseServer):
    """funcoin 命令行服务。"""

    def __init__(self) -> None:
        """初始化服务名称。"""
        super().__init__(server_name="funcoin")

    def update(self, args: object | None = None, **kwargs: object) -> None:
        """升级已安装的 funcoin 包。"""
        run_shell_list(["pip install funcoin -U"])

    def run(self, *args: object, **kwargs: object) -> None:
        """执行一次每日行情下载（等价于 `funcoin download` 的默认参数）。"""
        download_daily()


def funcoin() -> None:
    """启动 funcoin 命令行入口。"""
    server = FunCoin()
    app = server_parser(server)

    @app.command()
    def download(days: int = 365):
        download_daily(days=days)

    app()
