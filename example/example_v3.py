from datetime import datetime, timedelta

from funcoin.task.load import LoadKlineDailyTask

date = datetime.now()
for i in range(1, 300):
    date += timedelta(days=-1)
    ds = date.strftime("%Y-%m-%d")
    LoadKlineDailyTask().refresh(ds=ds)
