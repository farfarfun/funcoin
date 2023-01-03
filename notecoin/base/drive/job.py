import os

from notecoin.base.database.backup import backup_tables

sqlite_file = f'{os.path.abspath(os.path.dirname(__file__))}/notecoin.db'

backup_tables(sqlite_file)
