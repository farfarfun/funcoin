import subprocess
import sys

from funsecret import read_secret

try:
    from mysql_to_sqlite3.cli import MySQLtoSQLite
except Exception as e:
    print(e)
    subprocess.check_call([sys.executable, "-m", "pip", "install", "mysql-to-sqlite3"])
    from mysql_to_sqlite3.cli import MySQLtoSQLite


def backup_tables(sqlite_file):
    tran = MySQLtoSQLite(
        mysql_database="funcoin",
        mysql_user=read_secret(cate1='funcoin', cate2='dataset', cate3='mysql', cate4='user'),
        mysql_password=read_secret(cate1='funcoin', cate2='dataset', cate3='mysql', cate4='password'),
        mysql_host=read_secret(cate1='funcoin', cate2='dataset', cate3='mysql', cate4='host'),
        mysql_port=read_secret(cate1='funcoin', cate2='dataset', cate3='mysql', cate4='port'),
        mysql_tables=['funcoin_lanzou'],
        sqlite_file=sqlite_file)

    tran.transfer()
