import os
from datetime import timedelta
from datetime import datetime
import time
import pandas as pd
#from pathos.multiprocessing import ProcessingPool



path = '/media/ethan/MyData/Data/futures/backtest/'

symbol = 'rb'
begin_date=datetime(2014,1,1)
end_date = datetime(2019,11,30)
ys = range(begin_date.year, end_date.year+1)
years = []
for y in ys:
    years.append(y)

df = pd.concat([pd.read_csv(path+str(y)+'/'+symbol+'_main_force.csv',encoding='gbk',
                            index_col='datetime',) for y in years],
               sort=True)
df2 = df[['open','high','low','close','volume']]
df2.fillna(method='ffill',inplace=True)
df2.fillna(method='bfill',inplace=True)
df2['adjcoef']=df['adjcoef']
df2['adjcoef'][-1]= 1.0

df3 = df2.iloc[::-1]
df3['cumcoef'] = df3['adjcoef'].cumprod()
df4 = df3.iloc[::-1]
df4.fillna(method='bfill',inplace=True)

df5= df4[['open','high','low','close']].multiply(df4['cumcoef'], axis='index')
df5['volume'] = df4['volume']

df5.index = pd.to_datetime(df5.index.values)
df5 = df5[df5.index >=begin_date]
df5 = df5[df5.index <=end_date]
df5.index.name = 'datetime'

name = symbol + '_' + str(begin_date.date()) + '_' + str(end_date.date()) + '.csv'
df5.to_csv(name)