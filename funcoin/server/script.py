import os

from darkbuild.core.core import command_line_parser
from darkbuild.manage import BaseServer, ServerManage

# lsof -t -i:8444
# sudo kill -9 `sudo lsof -t -i:8444`


class CoinServer(BaseServer):
    def __init__(self):
        path = os.path.abspath(os.path.dirname(__file__))
        super(CoinServer, self).__init__("funcoin_server", path)

    def init(self):
        try:
            self.manage.init()
        except Exception as e:
            print(e)

        self.manage.add_job(
            server_name="funcoin_server",
            directory=self.current_path,
            command=f"python funcoin_server.py",
            user="bingtao",
            stdout_logfile="/fundata/logs/funcoin/server.log",
        )
        self.manage.start()


class CoinWorker(BaseServer):
    def __init__(self):
        path = os.path.abspath(os.path.dirname(__file__))
        super(CoinWorker, self).__init__("funcoin_worker", path)

    def init(self):
        try:
            self.manage.init()
        except Exception as e:
            print(e)

        self.manage.add_job(
            server_name="funcoin_worker",
            directory=self.current_path,
            command=f"celery -A funcoin_celery worker -l info",
            user="bingtao",
            stdout_logfile="/fundata/logs/funcoin/worker.log",
        )
        self.manage.start()


def funcoin_server():
    args = command_line_parser()
    package = CoinServer()
    if args.command == "init":
        package.init()
    elif args.command == "stop":
        package.stop()
    elif args.command == "start":
        package.start()
    elif args.command == "restart":
        package.restart()
    elif args.command == "help":
        info = """
        init
        stop
        start
        restart
        """
        print(info)


def funcoin_worker():
    args = command_line_parser()
    package = CoinWorker()
    if args.command == "init":
        package.init()
    elif args.command == "stop":
        package.stop()
    elif args.command == "start":
        package.start()
    elif args.command == "restart":
        package.restart()
    elif args.command == "help":
        info = """
        init
        stop
        start
        restart
        """
        print(info)
