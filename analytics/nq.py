import pandas as pd
import numpy as np
from datetime import datetime
import time
import yfinance as yf
pd.set_option('display.max_rows',500)
pd.set_option('display.max_columns',500)
pd.set_option('display.width',1000)
# yf.pdr_override()

etd_data = yf.Ticker("NQ=F")
df = etd_data.history(period="3d", interval="1h")
print(df)
# print(df.Close.diff(),321)
condition_up = df['Low'] > df['High'].shift()
condition_down = df['High'] > df['Low'].shift()

df['hop'] = np.nan

df.loc[condition_up, 'hop_up'] = -1
df.loc[condition_down, 'hop_down'] = 1

hop_record = []
print(df)
for i in range(len(df)):
    print("i: ", i )
    print(list(df['hop_up']))
    if list(df['hop_up'])[i] == -1:
        hop_date = df['Date'].at[i]
        ex_hop_price = df['High'].at(i-1)
        post_hop_price = df['Low'].at[i]
        fill_date = ''   # 回补时间
        for j in range(i, len(df)):
            if df['Low'].at[j] <= ex_hop_price:
                fill_date = df['Date'].at[j]
                break
        hop_record.append({'hop':'up',
                           'hop_date': hop_date,
                           'ex_hop_price': ex_hop_price,
                           'post_hop_price': post_hop_price,
                           'fill_date': fill_date,
                           })
    elif list(df['hop_down'])[i] == 1:

        for j in range(i, len(df)):
            if df['High'].at[j] <= ex_hop_price:
                fill_date = df['Date'].at[j]
                break
        hop_record.append({'hop':'down',
                           'hop_date': hop_date,
                           'ex_hop_price': ex_hop_price,
                           'post_hop_price': post_hop_price,
                           'fill_date': fill_date,
                           })


print(hop_record,'end')
# hop_df = pd.DataFrame(hop_record)
# print(hop_df)