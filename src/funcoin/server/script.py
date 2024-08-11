import argparse
from datetime import datetime

from ccxt import binance
from funcoin.base.drive.lanzou import LanzouDirectory
from funcoin.coins.base.file import DataFileProperty
from funcoin.task.load import LoadTask


def download(args):
    file_pro = DataFileProperty(exchange=binance(), path="tmp")
    file_pro.file_format = "%Y%m%d"
    file_pro.change_data_type(args.type)
    file_pro.change_timeframe(args.timeframe)
    file_pro.change_freq(args.freq)
    file_pro.load_days()


def sync(args):
    lanzou = LanzouDirectory(fid=6073401)
    lanzou.scan_all_file(clear=False)


def funcoin():
    parser = argparse.ArgumentParser(prog="PROG")
    subparsers = parser.add_subparsers(help="sub-command help")
    # 添加子命令
    download_parser = subparsers.add_parser("download", help="download data")
    download_parser.add_argument(
        "--type",
        nargs=2,
        metavar=("kline", "trade"),
        default="kline",
        help="What kind of data do you want to download",
    )

    download_parser.add_argument("-ds", default=None, help="the date")
    download_parser.add_argument("-days", default=7, type=int, help="the date")
    download_parser.add_argument("--freq", nargs=2, metavar=("daily", "dailys"), default="daily", help="")
    download_parser.add_argument("--timeframe", nargs=2, metavar=("1m", "5m"), default="1m", help="")
    download_parser.set_defaults(func=download)  # 设置默认函数

    # 添加子命令
    parser_s = subparsers.add_parser("sync", help="sub help")
    parser_s.set_defaults(func=sync)  # 设置默认函数
    args = parser.parse_args()
    args.func(args)
