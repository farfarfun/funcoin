from funserver.servers.base import BaseServer, server_parser
from funshell import run_shell_list

from funcoin.coins.task.download import download_daily


class FunCoinDownload(BaseServer):
    """每日行情下载服务。"""

    def __init__(self) -> None:
        """初始化服务名称。"""
        super().__init__(server_name="funcoin-download")

    def update(self, args: object | None = None, **kwargs: object) -> None:
        """升级已安装的 funcoin 包。"""
        run_shell_list(["pip install funcoin -U"])

    def run(self, *args: object, **kwargs: object) -> None:
        """执行一次每日行情下载。"""
        download_daily()


def funcoin_download() -> None:
    """启动命令行服务入口。"""
    server = FunCoinDownload()
    app = server_parser(server)
    app()
