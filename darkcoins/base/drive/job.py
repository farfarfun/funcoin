import os

from darkcoins.base.database.backup import backup_tables

sqlite_file = f'{os.path.abspath(os.path.dirname(__file__))}/darkcoins.db'

backup_tables(sqlite_file)
