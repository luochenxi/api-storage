import easyquotation
quotation = easyquotation.use('sina')
b = quotation.market_snapshot(prefix=True)
a = quotation.stocks(['AAPL'], prefix=True)
print(a)
print(len(b))
for i in b:
    print(i)