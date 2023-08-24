from funcoin.task.load import LoadKlineDailyTask
from funsecret import read_secret


LoadKlineDailyTask().refresh(ds="2023-08-01")
