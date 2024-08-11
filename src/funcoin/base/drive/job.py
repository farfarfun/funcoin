import os

from funcoin.base.database.backup import backup_tables

sqlite_file = f'{os.path.abspath(os.path.dirname(__file__))}/funcoin.db'

backup_tables(sqlite_file)
