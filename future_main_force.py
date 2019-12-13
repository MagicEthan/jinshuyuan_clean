import pandas as pd
import numpy as np
import os
from datetime import timedelta
import datetime
import time
#from pathos.multiprocessing import ProcessingPool
year = '2019'
path = '/media/ethan/MyData/Data/futures/FutAC_Min1_Std_'+year+'/'
import copy
files_all = os.listdir(path)
files_to_process = [file for file in files_all if file[2].isdigit()]
import sys
# Get all symbols available for processing
symbols = set()
for f in files_to_process:
    symbol_name_exp= f.split('.')[0]
    t = [s for s in symbol_name_exp if (not s.isdigit())]
    symbols.add(''.join(t))

#symbol = 'rb'

def gen_mf_data(symbol):
    #print(str(''.join(c for c in file if c.isalpha())))
    def compare(file, s):
        #for file in files_to_process:
        file = file.split('.')[0]
        x = str(''.join(c for c in file if c.isalpha()))
        return x==s
    #print(files_to_process)
    symbol_files = [file for file in files_to_process if compare(file, symbol)]
    print("所有合约文件： ", symbol_files)

    df = pd.concat([pd.read_csv(path+f,encoding='gbk', index_col='时间') for f in symbol_files],axis=1,sort=True)
    #df.to_csv('test2015.csv')
    df.index = pd.to_datetime(df.index.values)
    mf_yesterday = symbol_files[0].split('.')[0]

    mf_dict = {}
    j = 0
    for index, row in df.iterrows():
        if index.hour == 15 and index.minute == 0:
            print('Market Closed', index)
            main_force={}
            for k,v in zip(row[1::9],row[8::9]):
                if v>=0:
                    main_force[k] = v
            mf = sorted(main_force.items(), key=lambda item:item[1], reverse=True)
            mf_contract = mf[0][0]
            if j>0:
                print('Main Contract: ', mf_contract)

                mf_number = "".join(x for x in mf_contract if x.isdigit())
                mf_yesterday_number = "".join(x for x in mf_yesterday if x.isdigit())

                if len(mf_number) == 3:
                    if mf_number[0] != "0":
                        mf_number = "1"+mf_number
                    elif mf_number[0] == '0':
                        mf_number = "2"+mf_number

                if len(mf_yesterday_number) == 3:
                    if mf_yesterday_number[0] != "0":
                        mf_yesterday_number = "1"+mf_yesterday_number
                    elif mf_yesterday_number[0] == '0':
                        mf_yesterday_number = "2"+mf_yesterday_number

                if int(mf_number) < int(mf_yesterday_number):
                    print("Main Contract Has Been Changed Yesterday! Keep the Same!")
                    with open("main_force_log_"+year+".txt","a") as f:
                        f.writelines(str(index)+" main contract swinging " + str(symbol) + "\n")
                    mf_contract = mf_yesterday
            j+=1
            mf_dict[index.to_pydatetime().date()] = mf_contract
            mf_yesterday = mf_contract

        # if the start data has night session
        if index.hour == 0:
            mf_dict[index.to_pydatetime().date()] = mf_yesterday

    df_mf = pd.DataFrame([],columns=['datetime','symbol','open','high','low','close','volume','turnover','openinterest','adjcoef'])
    coef = None
    i=0
    old_mf=None
    today_change=False
    for index, row in df.iterrows():

        mf = mf_dict[index.to_pydatetime().date()]

        datats = [index.to_pydatetime()]

        datalist = row.tolist()
        n = datalist.index(mf)
        data = datats + datalist[n:n + 8] + [coef]
        if i>0:

            if not today_change and mf!=mf_last:
                today_change=True
                old_mf = mf_last
            if today_change and index.hour < 15:
                try:
                    last_n = datalist.index(old_mf)
                except:
                    print("Old contract expired, or this is a dumb symbol, or our method doesn't apply.")
                    with open("main_force_log_" + year + ".txt", "a") as f:
                        f.writelines(str(index) + str(symbol) + str(ValueError) + "\n")
                        f.writelines(str(index) + " Old contract expired, or this is a dumb symbol, or our method doesn't apply. " + str(symbol) + "\n")
                    print("EXIT PROCESS.")
                    time.sleep(3)
                    sys.exit(0)
                data = datats+datalist[last_n:last_n+8] +[coef]

            if today_change and index.hour==15 and index.minute==0:
                new_close = float(datalist[n+4])
                old_close = float(datalist[last_n+4])
                coef = new_close/old_close
                print(str(index), "old MF close: ", old_mf, old_close,
                      "new MF close: ", new_close, new_close, "adjust coef: ", coef)
                today_change=False
                old_mf=None
                data = datats+datalist[last_n:last_n+8] +[coef]

        print("主力数据： " , data)
        i+=1
        data = pd.Series(data, index=df_mf.columns)
        df_mf = df_mf.append(data, ignore_index=True)
        mf_last = mf
        coef=None

    df_mf.to_csv("./"+year+"/"+symbol+'_main_force.csv',index=False)
    print("Done!!!")
    return None

#gen_mf_data('TA')
from multiprocessing import Pool

if __name__ == "__main__":
    p = Pool(processes=6, maxtasksperchild=10)
    for s in symbols:

        p.map_async(gen_mf_data, (s,))
    p.close()
    p.join()