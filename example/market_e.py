from funcoin.huobi.client.market import MarketClient

client = MarketClient()
tickers = client.get_market_tickers()


print(tickers)

print(tickers.data)
print("end")
