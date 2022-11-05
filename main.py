#!/usr/bin/env python
# coding: utf-8

### need to install
# !pip install pyTelegramBotAPI

## import modules
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, date, time
from urllib.request import urlopen
import ssl

import sys
import os
import telebot
import config


def select_year_month(datastring):
    return datastring.split()[0][-4:] + '_' + datastring[3:5]

def print_message(message, end=''):
    print(message)
    flog.write(message + end)


## open log file
flog = open('log.txt','a')
flog.write(str(datetime.now()) + '   ')
print("now: ", datetime.now())


### This code makes the verification undone so that the ssl certification is not verified.
try:
   _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    # Legacy Python that doesn't verify HTTPS certificates by default
    pass
else:
    # Handle target environment that doesn't support HTTPS verification
    ssl._create_default_https_context = _create_unverified_https_context


## read url
urlname = "https://mosecom.mos.ru/mgu/"
try:
    html = urlopen(urlname).read().decode('utf-8')
except:
    text = "No access to the site  " + urlname
    print_message(text, "\n")
    flog.close()

    ## send alarm to info channel by bot
    bot = telebot.TeleBot(config.token, parse_mode=None)
    bot.send_message(config.channel, text)

    ## работаем ли мы в питоне или notebook? 
    sys.exit("No access to the site  " + urlname + "\n")


## parse html and find 'AirCharts.init' chart 
soup = BeautifulSoup(html, 'html.parser')
for link in soup.find_all('script'):
    w = str(link)
    if 'AirCharts' in w:
        chart = w    
        break

## make dict d from text chart
start = chart.find('{')
stop = chart[: chart.find('{"month')].rfind(',')
comm = 'd = ' + chart[start : stop]
comm = comm.replace('null', '0')
exec(comm) ## make dict d form string
# print(type(d), list(key for key in d.keys()))


## collect data to table
rows_list = []
dates = dict()
for i in range(len(d['units']['h']['CH4']['data'])):
    array = []
    sectime = d['units']['h']['CH4']['data'][i][0] // 1000
    dt = datetime.utcfromtimestamp(sectime)
    print(i, sectime, dt.strftime("%d.%m.%Y %H:%M")) #, end=' ')
    array.append(sectime)
    array.append(dt.strftime("%d.%m.%Y %H:%M"))
    ## !!!!!!!!!!!!!!!!! утрать !!!!!!!!!!!
    #array.append(dt.strftime("%d.%m.%Y"))
    #array.append(int(dt.strftime("%H")))
    ## !!!!!!!!!!!!!!!!!
    # update dates set
    year = array[1].split()[0][-4:]
    month = array[1][3:5]
    dates[year] = dates.get(year, set())
    dates[year].add(month) 

    ## fill row array with detectors data
    for param in d['units']['h'].keys():
        #print(d['units']['h'][param]['data'][i][1], end=' ')
        array.append(d['units']['h'][param]['data'][i][1])
    #print(array)

    ## add row to the table rows_list
    rows_list.append(array)

##print(*rows_list, sep="\n")

## sys.exit("Test stop running")
## преобразовать данные в dataframe df
colnames = d['units']['h'].keys()
colnames = ['timestamp', 'datetime'] + list(colnames)
df = pd.DataFrame(rows_list, columns=colnames)


###################################################################
## read existing xls file and add new data to dataframe from excel file
dirname = './data/'

## если папки нет, нужно ее создать
try:
    os.stat(dirname)
except OSError:
    os.mkdir(dirname)


filename = 'mav_mos_mgu'  #'mav_mos_mgu.xlsx'
for year in dates:
    for month in dates[year]:
        ym_pattern = year + '_' + month
        #print(ym_pattern, end=' ')
        filenamexls = dirname + ym_pattern + '_' + filename + ".xlsx"
        filenamecsv = dirname + ym_pattern + '_' + filename + ".csv"

        ## проверить, существует ли файл
        nofile = False
        try:
            os.stat(filenamexls)
        except:
            nofile = True
        #print(nofile)

        ## выбрать строки за нужный месяц и год
        dfsave = df[df.datetime.apply(select_year_month) == ym_pattern]
        print("For pattern ", ym_pattern, " there are data with shape:", dfsave.shape)

        newlines = 0
        action = "written"
        ## если файла с данными нет - запишем все в новый файл
        if nofile:
            newlines = dfsave.shape[0]
            text = "Excel file " + filenamexls + " not found. New file will created."
            print_message(text, "\n")
        ## если файл есть - считать данные из существующего файла и дополнить их
        else:
            try:  ## файл доступен:
                ## read dataset from file
                df0 = pd.read_excel(filenamexls)
                # добавить новые строки к старым, выбросить все повторяющиеся, оставить только новые строки
                df1 = df0.append(dfsave).drop_duplicates(keep=False)
                # добавить новые строки в конец датасета из файла
                dfsave = df0.append(df1, ignore_index=True).drop_duplicates()
                newlines = dfsave.shape[0] - df0.shape[0]
                action = "added"
            except:
                ## файл с данными недоступен - запишем все в новый файл с временным именем
                newlines = dfsave.shape[0]
                text = "Data file " + filenamexls + "not available. File with new name will created."
                print_message(text, '\n')
                ## создать имя для нового файла
                timestr = "_".join(str(datetime.now()).split())
                filenamexls = filenamexls[:-4] + timestr + ".xlsx"
                filenamecsv = filenamecsv[:-3] + timestr + ".csv"


        ## save results to excel file
        #df.set_index('timestamp').to_excel(filename)
        if newlines:
            dfsave.set_index('timestamp').to_excel(filenamexls)
            dfsave.set_index('timestamp').to_csv(filenamecsv)
            print(ym_pattern, newlines, " lines saved to", filenamexls)

            text = str(newlines) + " lines " + action + " to file " + filenamexls
            print_message(text, '\n')
        else:
            text = "No new data to add to file " + filenamexls
            print_message(text, '\n')


## close log file
#flog.write('\n')
flog.close()

