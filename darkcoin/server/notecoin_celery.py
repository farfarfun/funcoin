from noteworker import get_default_app

app=get_default_app()
app.autodiscover_tasks(packages=['darkcoin.strategy.binance.auto'])
