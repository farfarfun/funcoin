from funshell import run_shell_list
from funcoin.coins.task.download import download_daily
from funserver.servers.base import BaseServer, server_parser


class FunCoin(BaseServer):
    def __init__(self):
        super().__init__(server_name="funcoin")

    def update(self, args=None, **kwargs):
        run_shell_list(["pip install funcoin -U"])

    def run(self, *args, **kwargs):
        """作为服务运行时执行一次每日行情下载（等价于 `funcoin download` 的默认参数）。"""
        download_daily()


def funcoin():
    server = FunCoin()
    app = server_parser(server)

    @app.command()
    def download(days: int = 365):
        download_daily(days=days)

    app()
