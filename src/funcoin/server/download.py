from funbuild.shell import run_shell_list
from funcoin.coins.task.download import download_daily
from funserver.base import BaseServer, server_parser


class FunCoinDownload(BaseServer):
    def __init__(self):
        super().__init__(server_name="funcoin-download")

    def update(self, args=None, **kwargs):
        run_shell_list(["pip install funcoin -U"])

    def run(self, *args, **kwargs):
        download_daily()


def funcoin_download():
    server = FunCoinDownload()
    parser, subparsers = server_parser(server)

    args = parser.parse_args()
    params = vars(args)
    args.func(**params)
