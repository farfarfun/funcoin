# nohup /home/bingtao/opt/miniconda3/bin/python /home/bingtao/workspace/hubs/funcoin/example/example_v3.py  >>./logs/fundev/fundev-$(date +%Y-%m-%d).log 2>&1 &
from datetime import datetime, timedelta

from funcoin.task.load import LoadKlineDailyTask

date = datetime.now()
for i in range(1, 3650):
    date += timedelta(days=-1)
    ds = date.strftime("%Y-%m-%d")
    LoadKlineDailyTask().refresh(ds=ds)
