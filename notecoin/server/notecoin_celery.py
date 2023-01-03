from noteworker import get_default_app

app=get_default_app()
app.autodiscover_tasks(packages=['notecoin.strategy.binance.auto'])
