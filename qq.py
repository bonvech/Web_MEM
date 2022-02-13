#!/usr/bin/env python
# coding: utf-8

# In[25]:


## import modules
import pandas as pd
from urllib.request import urlopen
from bs4 import BeautifulSoup
from datetime import datetime, date, time
from urllib.request import urlopen

import sys

f = open('log.txt','a')

print(sys.version)
print(datetime.now())

f.write(sys.version +'\n')
f.write(str(datetime.now()) + '\n')

## read url
html = urlopen("https://mosecom.mos.ru/mgu/").read().decode('utf-8')
#html = urlopen("https://mosecom.mos.ru/mgu/").read()
#print(html)


## parse html and find 'AirCharts.init' chart 
soup = BeautifulSoup(html, 'html.parser')
#print (soup)
for link in soup.find_all('script'):
#    print(link)
#    w = link.get_text()
    w = str(link)
 #   w = link
 #   print(w)
    if 'AirCharts' in w:
        chart = w    
 #       print(w)
        break

# make dict from text chart
start = chart.find('{')
stop = chart[: chart.find('{"month')].rfind(',')
comm = 'd = ' + chart[start : stop]
comm = comm.replace('null', '0')
exec(comm)
# print(type(d), list(key for key in d.keys()))


# collect data to table
rows_list = []
for i in range(len(d['units']['h']['CH4']['data'])):
    array = []
    sectime = d['units']['h']['CH4']['data'][i][0] // 1000
    dt = datetime.utcfromtimestamp(sectime)
    #print(i, sectime, dt.strftime("%d.%m.%Y"), dt.strftime("%H"), end=' ')
    array.append(sectime)
    array.append(dt.strftime("%d.%m.%Y"))
    array.append(int(dt.strftime("%H")))
    #print(dt)
    for param in d['units']['h'].keys():
        #print(d['units']['h'][param]['data'][i][1], end=' ')
        array.append(d['units']['h'][param]['data'][i][1])
    #print(array)
    rows_list.append(array)


## записать данные в dataframe
colnames = d['units']['h'].keys()
colnames = ['timestamp', 'date', 'hour'] + list(colnames)
df = pd.DataFrame(rows_list, columns=colnames)

## read existing xls file and add new data to dataframe from file
filename = 'mav_mos_mgu.xlsx'
try:
    df0 = pd.read_excel(filename) #, index_col=0)
    # выбрать новые строки
    df1 = df0.append(df).drop_duplicates(keep=False)
    # добавить новые строки к старым из файла
    df = df0.append(df1, ignore_index=True).drop_duplicates()
    print(df.shape[0] - df0.shape[0], "new lines added to", filename)

    f.write(str(df.shape[0] - df0.shape[0]) + "  new lines added to  " + filename + '\n')
except:
    print("Excel file", filename, "not found")
    print(df.shape[0], "lines writen to file to ", filename)

## save results to excel file
df.set_index('timestamp').to_excel(filename)

f.close()

