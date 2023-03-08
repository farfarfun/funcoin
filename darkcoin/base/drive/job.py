import os

from darkcoin.base.database.backup import backup_tables

sqlite_file = f'{os.path.abspath(os.path.dirname(__file__))}/darkcoin.db'

backup_tables(sqlite_file)
