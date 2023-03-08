import uvicorn
from fastapi import FastAPI
from darkcoins.server.download import DownloadServer
from darkcoins.server.strategy import StrategyServer

app = FastAPI()

app.include_router(DownloadServer())
app.include_router(StrategyServer())


uvicorn.run(app, host='0.0.0.0', port=8451)
