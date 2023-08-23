import ccxt
from funcoin.coins.base.cron import load_month
from funsecret.secret import read_secret, write_secret
from funsecret import read_secret

path_root = read_secret(cate1="funcoin", cate2="path", cate3="local", cate4="path_root") or "~/workspace/tmp"

load_month(ccxt.okex(), tmp_path=path_root)
