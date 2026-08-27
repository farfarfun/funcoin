from funshell import run_shell_list
from funcoin.coins.task.download import download_daily
from funserver.servers.base import BaseServer, server_parser


class FunCoinDownload(BaseServer):
    def __init__(self):
        super().__init__(server_name="funcoin-download")

    def update(self, args=None, **kwargs):
        run_shell_list(["pip install funcoin -U"])

    def run(self, *args, **kwargs):
        download_daily()


def funcoin_download():
    server = FunCoinDownload()
    app = server_parser(server)
    app()
