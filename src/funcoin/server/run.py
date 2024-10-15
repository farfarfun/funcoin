from funbuild.shell import run_shell_list
from funcoin.coins.task.download import download_daily
from funserver.base import BaseServer, server_parser


class FunCoin(BaseServer):
    def __init__(self):
        super().__init__(server_name="funcoin")

    def update(self, args=None, **kwargs):
        run_shell_list(["pip install funcoin -U"])

    def run(self, *args, **kwargs):
        pass


def funcoin():
    server = FunCoin()
    parser, subparsers = server_parser(server)

    build_parser1 = subparsers.add_parser("download", help="download daily")
    build_parser1.add_argument("--days", default=365, help="days")
    build_parser1.set_defaults(func=download_daily)

    args = parser.parse_args()
    params = vars(args)
    args.func(**params)
