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
    with open(logfilename,'a') as flog:
        flog.write(str(datetime.now()) + '   ')
        flog.write(message + end)

## ----------------------------------------------------------------
##  write message to bot
## ----------------------------------------------------------------
def write_to_bot(text):
    try:
        bot = telebot.TeleBot(config.token, parse_mode=None)
        bot.send_message(config.channel, text)
    except Exception as err:
        ##  напечатать строку ошибки
        text = f": ERROR in writing to bot: {err}"
        print_message(text)  ## write to log file


###################################################################
###################################################################
dirname = './data/'
filename_prefix = 'mav_mos_mgu'  
logfilename = dirname + "_".join(["_".join(str(datetime.now()).split('-')[:2]), filename_prefix,  'log.txt'])
print("now: ", datetime.now())

##  если папки c данными нет, нужно ее создать
try:
    os.stat(dirname)
except OSError:
    os.mkdir(dirname)


####################################
##  This code makes the verification undone so that the ssl certification is not verified.
try:
   _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    ## Legacy Python that doesn't verify HTTPS certificates by default
    message = "Legacy Python that doesn't verify HTTPS certificates by default"
    print_message(message, end='\n')
else:
    ## Handle target environment that doesn't support HTTPS verification
    ssl._create_default_https_context = _create_unverified_https_context


####################################
##  open url
urlname = "https://mosecom.mos.ru/mgu/"
try:
    html = urlopen(urlname).read().decode('utf-8')
except:
    text = "No access to the site  " + urlname
    print_message(text, "\n")
    ## send alarm to info channel by bot
    write_to_bot(text)
    
    ## работаем ли мы в питоне или notebook? 
    sys.exit("No access to the site  " + urlname + "\n")


####################################
##  parse html and find 'AirCharts.init' chart 
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
#print(type(d), list(key for key in d.keys()))
#print(type(d), list(key for key in d['units']['h'].keys()))

datum = d['units']['h']


####################################
## collect data to table

colnames = ['timestamp', 'datetime'] + list(datum.keys())
standart_columns = ['timestamp','datetime','CH4','CO','NO','NO2','OZ','PM10','PM2.5','SO2']
columns = standart_columns[:]
extra_columns = list(set(colnames) - (set(colnames) & set(standart_columns)))
if extra_columns:
    text = f"Warning! New parameter in site {urlname} data: {extra_columns}"
    write_to_bot(text)
    columns = standart_columns + extra_columns
    #print(columns)
    
##  create new dataframe with all columns
df = pd.DataFrame(columns=columns)
dates = dict()  ##  like {'2023': {'10'}}

some_key = list(datum.keys())[0] ## первый  параметр в списке
print('column keys:',  list(datum.keys()))
for i in range(len(datum[some_key]['data'])): ## читать все строки
    array = dict()
    sectime = datum[some_key]['data'][i][0] // 1000
    dt = datetime.utcfromtimestamp(sectime)
    #print(i, sectime, dt.strftime("%d.%m.%Y %H:%M")) #, end=' ')
    array['timestamp'] = sectime
    array['datetime']  = dt.strftime("%d.%m.%Y %H:%M")
    
    ## update set of dates
    year  = array['datetime'].split()[0][-4:]
    month = array['datetime'][3:5]
    dates[year] = dates.get(year, set())
    dates[year].add(month) 

    ## fill row array with detectors data
    nulls = 0
    for param in datum.keys():
        value = datum[param]['data'][i][1]
        array[param] = value
        if value == 0:
            nulls += 1
    
    #print("==>", array)
    if len(datum.keys()) == nulls: 
        continue

    ## add row to the dataframe
    df = pd.concat([df, pd.Series(array).to_frame().T], ignore_index=True)

#print(dates)
#print(df)
##sys.exit("Test stop running")


####################################
##  read existing csv file and add new data to dataframe from csv file
for year in dates:
    for month in dates[year]:
        ym_pattern = year + '_' + month
        filenamexls = dirname + ym_pattern + '_' + filename_prefix + ".xlsx"
        filenamecsv = dirname + ym_pattern + '_' + filename_prefix + ".csv"

        ##  проверить, существует ли файл
        nofile = False
        try:
            os.stat(filenamecsv)
        except:
            nofile = True

        ## выбрать строки за нужный месяц и год
        dfsave = df[df.datetime.apply(select_year_month) == ym_pattern]
        print("For pattern ", ym_pattern, " there are data with shape:", dfsave.shape)

        newlines = dfsave.shape[0]    
        action = "written"
        ## если файла с данными нет - запишем все в новый файл
        if nofile:
            newlines = dfsave.shape[0]
            text = "Data file " + filenamecsv + " not found. New file created."
            print_message(text, "\n")
            ## send alarm to info channel by bot
            write_to_bot(text)
            
        ## если файл есть - считать данные из существующего файла и дополнить их
        else:
            try:  ##  файл доступен:
                ##  read dataset from file
                df0 = pd.read_csv(filenamecsv)
                ##  добавить новые строки в конец датасета из файла, выбросить все повторяющиеся
                dfsave = pd.concat([dfsave, df0], ignore_index=True)\
                        .drop_duplicates()\
                        .sort_values(by=['timestamp'])
                newlines = dfsave.shape[0] - df0.shape[0]
                action = "added"
            except:
                ## файл с данными недоступен - запишем все в новый файл с временным именем
                text = f"Data file {filenamecsv} is not available. " 
                ## создать имя для нового файла
                timestr = "_".join(str(datetime.now()).replace(':','_').split())
                filenamexls = filenamexls[:-4] + timestr + ".xlsx"
                filenamecsv = filenamecsv[:-3] + timestr + ".csv"
                
                text = text + f"New file {filenamecsv} will created."
                print_message(text, '\n')
                ## send alarm to info channel by bot
                write_to_bot(text)
                

        ## save results to excel file
        if newlines:
            #print(dfsave)
            dfsave.to_csv(  filenamecsv, index=False)
            dfsave.to_excel(filenamexls, index=False, sheet_name=ym_pattern)
            #print(ym_pattern, newlines, " lines saved to", filenamecsv)

            text = str(newlines) + " lines " + action + " to file " + filenamecsv
            print_message(text, '\n')
        else:
            text = "No new data to add to file " + filenamecsv
            print_message(text, '\n')
