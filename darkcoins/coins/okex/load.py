import ccxt
from darkcoins.coins.base.cron import load_month
from notesecret.secret import read_secret, write_secret

path_root = '/home/bingtao/workspace/tmp/darkcoins/'

load_month(ccxt.okex(), tmp_path=path_root)
