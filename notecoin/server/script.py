import os

from notebuild.core.core import command_line_parser
from notebuild.manage import BaseServer, ServerManage

# lsof -t -i:8444
# sudo kill -9 `sudo lsof -t -i:8444`


class CoinServer(BaseServer):
    def __init__(self):
        path = os.path.abspath(os.path.dirname(__file__))
        super(CoinServer, self).__init__('notecoin_server', path)

    def init(self):
        try:
            self.manage.init()
        except Exception as e:
            print(e)

        self.manage.add_job(server_name='notecoin_server',
                            directory=self.current_path,
                            command=f"python notecoin_server.py",
                            user='bingtao',
                            stdout_logfile="/notechats/logs/notecoin/server.log")
        self.manage.start()


class CoinWorker(BaseServer):
    def __init__(self):
        path = os.path.abspath(os.path.dirname(__file__))
        super(CoinWorker, self).__init__('notecoin_worker', path)

    def init(self):
        try:
            self.manage.init()
        except Exception as e:
            print(e)

        self.manage.add_job(server_name='notecoin_worker',
                            directory=self.current_path,
                            command=f"celery -A notecoin_celery worker -l info",
                            user='bingtao',
                            stdout_logfile="/notechats/logs/notecoin/worker.log")
        self.manage.start()


def notecoin_server():
    args = command_line_parser()
    package = CoinServer()
    if args.command == 'init':
        package.init()
    elif args.command == 'stop':
        package.stop()
    elif args.command == 'start':
        package.start()
    elif args.command == 'restart':
        package.restart()
    elif args.command == 'help':
        info = """
        init
        stop
        start
        restart
        """
        print(info)


def notecoin_worker():
    args = command_line_parser()
    package = CoinWorker()
    if args.command == 'init':
        package.init()
    elif args.command == 'stop':
        package.stop()
    elif args.command == 'start':
        package.start()
    elif args.command == 'restart':
        package.restart()
    elif args.command == 'help':
        info = """
        init
        stop
        start
        restart
        """
        print(info)
