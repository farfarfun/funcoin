#  ps -ef | grep cron

# cd /home/bingtao/workspace/notechats/notecoin/notecoin/coins/base
nohup /home/bingtao/opt/anaconda3/bin/python /home/bingtao/workspace/notechats/notecoin/notecoin/coins/base/cron.py >>/notechats/notecoin/logs/notecoin-$(date +%Y-%m-%d).log 2>&1 &
tail -f /notechats/notecoin/logs/notecoin-$(date +%Y-%m-%d).log
