import pandas as pd
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
from   matplotlib import dates
import os
#import datetime
from datetime import datetime


############################################################################
## make resample by three close measurements
############################################################################ 
def average_by_three(datum):
    if 'timestamp' in datum.columns:
        datum = datum.rename(columns={"timestamp": "dt"}, errors="raise")
        datum.set_index('dt', inplace=True)
        datum.index = pd.to_datetime(datum.index, unit='s')
        #datum['dt'] = datum['timestamp']
    else:
        if 'Date' in datum.columns:
            datum['dt'] = datum['Date'] + ' ' + datum['Time (Moscow)']
            fmt = '%Y/%m/%d %H:%M:%S'
        else:
            datum['dt'] = datum['datetime']
            fmt = '%d.%m.%Y %H:%M'
        datum.set_index('dt', inplace=True)
        datum.index = pd.to_datetime(datum.index, format=fmt) # format='%m/%d%Y %-I%M%S %p'        

    datum = datum.drop(['datetime'], axis=1)
    #print(datum)
        
    #return datum.resample("3T").sum().fillna().rolling(window=3, min_periods=1).mean()
    #return datum.resample("3H").mean().rolling(window=3, min_periods=1).mean()
    return datum.resample("3h").mean()
    #return datum.resample("3h").mean().rolling(window=3, min_periods=1).mean()


############################################################################
##  Return possible datatime format
############################################################################
def get_time_format():
    ##  check if format is possible
    fmt = dates.DateFormatter('%d-%2m-%Y\n %H:%M')
    try:
        print(datetime.now().strftime(fmt))
    except:
        fmt = dates.DateFormatter('%d/%m/%Y\n %H:%M')
    return fmt


############################################################################
##  Get folder separator sign
############################################################################
def get_folder_separator():

    if 'ix' in os.name:
        sep = '/'  ## -- path separator for LINIX
    else:
        sep = '\\' ## -- path separator for Windows
    return sep


############################################################################
##  Prepare data to plot graphs
############################################################################
def get_year_from_filename(name):
    if 'ix' in os.name:
        sep = '/'  ## -- path separator for LINIX
    else:
        sep = '\\' ## -- path separator for Windows

    year = name.split(sep)[-1].split('_')[0]
    month = name.split(sep)[-1].split('_')[1]
    return int(year), int(month)


############################################################################
##  Get data from previous_month
############################################################################
def get_data_from_previous_month(name):
    sep = get_folder_separator()

    ##  get actual year and month
    year, month = get_year_from_filename(name)

    ##  calculate previous year and month
    newmonth = month - 1 if month > 2 else 12
    newyear = year - 1 if month == 1 else year
    #print(newyear, newmonth)

    ##  replace year and month in filename
    if debug_mode:
        print(name)
    nparts = name.split(sep)
    nfile = nparts[-1].split('_')
    nfile[0] = str(newyear)
    nfile[1] = f'{newmonth:02d}'
    nparts[-1] = '_'.join(nfile)
    newname = sep.join(nparts)
    #print(newname)

    ## check data file
    if not os.path.exists(newname):
        if debug_mode:
            print(newname, "is not found")
        return -1
    else:
        if debug_mode:
            print(newname, "exists")

    ## get previous month data
    #data = pd.read_excel(newname)
    data = pd.read_csv(newname)

    return data



############################################################################
##  Prepare data to plot graphs
##  get 2 week data from data files
############################################################################
def prepare_data(xlsfilename):
    #data = pd.read_excel(xlsfilename)
    data = pd.read_csv(xlsfilename)
    #print(data.columns)

    ##  make column to plot on x axis
    if 'Date' in data.columns:
        x = (data['Date'].astype('string') + ' ' + data['Time (Moscow)'].astype('string'))
        x = pd.to_datetime(x, format='%Y/%m/%d %H:%M:%S')
    else:
        x = pd.to_datetime(data['datetime'], format='%d.%m.%Y %H:%M')
    #print(x)

    ## если данных меньше, чем 2 недели, считать данные за прошлый месяц
    if x.min() + pd.to_timedelta("336:00:00") > x.max():
        if debug_mode:
            print("Data file has less than 2 week data")
        olddata = get_data_from_previous_month(xlsfilename)
        if type(olddata) != int:
            data = pd.concat([olddata, data], ignore_index=True)
        #print(f"joined data: {data.shape}\n", data.head())
        ## make column to plot on x axis
        if 'Date' in data.columns:
            x = (data['Date'].astype('string') + ' ' + data['Time (Moscow)'].astype('string'))
            x = pd.to_datetime(x, format='%Y/%m/%d %H:%M:%S')
        else:
            x = pd.to_datetime(data['datetime'], format='%d.%m.%Y %H:%M')
    else:
        if debug_mode:
            print("One file is enouth")

    data['plotx'] = x

    ##  оставить только две недели
    xmin = x.max() - pd.to_timedelta("336:00:00") ## 14 days
    #print("xmin: ", xmin)
    data = data[pd.to_datetime(data['plotx']) > xmin]
    #print(f"only 2 weeks: {data.shape}\n", data.head())

    ## убрать повторы
    data = data.drop_duplicates(keep='first')

    ## отсортировать данные по времени
    data = data.sort_values(by='timestamp', ascending=True)
    #print(data.head(20))

    return data



############################################################################
## Create plots from excel file with Aethalometer data
#  @param nfigs - number of files to create
############################################################################
def plot_four_figures_from_excel(datum, path_to_figures, nfigs=1, name='figure', title="Web_MSU"):
    if debug_mode:
        print(f"Plot  {nfigs}  figures")

    #print(path_to_figures)
    if not os.path.isdir(path_to_figures):
        os.makedirs(path_to_figures)

    ## format graph
    fmt = get_time_format()
    locator = matplotlib.dates.AutoDateLocator(minticks=20, maxticks=30)
    labelrotation=0
    facecolor = 'white'
    plt.rcParams['xtick.labelsize'] = 10
    plt.rcParams['lines.linewidth'] = 3

    columns = list(datum.columns[2:])
    columns.remove('plotx')
    columns2 = ['PM10', 'PM2.5']
    columns.remove('PM10')
    columns.remove('PM2.5')
    #print(columns)

    ## get 2 days data
    xmin = datum.plotx.max() - pd.to_timedelta("48:01:00")  ##  2 days
    #print("xmin: ", xmin)
    data = datum[datum.plotx >= xmin]
    x = data['plotx']
    xlims = (x.min(), x.max() + pd.to_timedelta("2:00:00"))

    ##########################
    ## Figure1: ['CH4', 'CO', 'NO', 'NO2', 'OZ', 'SO2']
    if nfigs == 1:
        fig = plt.figure(figsize=(16, 12))
        ax_1 = fig.add_subplot(3, 1, 1)
    else:
        fig = plt.figure(figsize=(10, 5))
        ax_1 = fig.add_subplot(1, 1, 1)

    for i in range(len(columns)):
        wave = columns[i]   
        #print(wave)
        y = data[wave].replace(np.nan, 0)
        #print(wave, y.shape, all(y.values), set(y.values))
        if not any(y.values):
            continue        
        y = data[wave].replace(0, np.nan)
        ## plot
        if wave == 'CH4':
            y = y / 100
            ax_1.plot(x, y, label=wave + ' / 100') #color='red',
        elif wave == 'CO':
            y = y / 10
            ax_1.plot(x, y, label=wave + ' / 10')    
        elif wave == 'SO2':
            y = y * 10
            ax_1.plot(x, y, label=wave + ' * 10')    
        #elif i == 5:
        #    ax_1.plot(x, y, color='black', label=wave)
        else:
            ax_1.plot(x, y, label=wave)

    ax_1.set_xlim(xlims)
    ax_1.set_ylim(bottom=0)
    ax_1.legend()
    ax_1.set_title(title, loc='right')
   
    # Повернем метки рисок на 55 градусов
    ax_1.tick_params(axis='x', labelrotation=labelrotation)
    
    ax_1.xaxis.set_major_formatter(fmt)
    ax_1.xaxis.set_minor_locator(locator)
    ax_1.grid(which='major', alpha=0.9)
    ax_1.grid(which='minor', alpha=0.5, linestyle='--')


    ## save to files
    if nfigs != 1:
        plotname = path_to_figures + name + '_all_day'
        fig.savefig(plotname + '.svg', facecolor=facecolor, bbox_inches='tight')
        fig.savefig(plotname + '.png', facecolor=facecolor, bbox_inches='tight') 


    ##########################
    ## Figure 2: PM10, PM2
    if nfigs == 1:
        ax_2 = fig.add_subplot(3, 1, 2)
    else:
        fig = plt.figure(figsize=(10, 5))
        ax_2 = fig.add_subplot(1, 1, 1)

    for wave in ['PM10', 'PM2.5']:
        #print(wave)
        y = data[wave].replace(0, np.nan)
        #if i == 0:  # !!! "BCff"
        if wave == 'PM10':
            color = 'dimgray'
        else:      
            color = 'mediumorchid'
        ax_2.plot(x, y, color=color, label=wave)
        #ax_2.fill_between(x, y, np.zeros_like(y), color=color)

    ax_2.set_xlim(xlims)
    ax_2.set_ylim(bottom=0)
    ax_2.legend()
    
    ax_2.tick_params(axis='x', labelrotation=labelrotation)
    
    ax_2.xaxis.set_major_formatter(fmt)
    ax_2.xaxis.set_minor_locator(locator)
    ax_2.grid(which='major', alpha=0.9)
    ax_2.grid(which='minor', alpha=0.5, linestyle='--')

    ## save to file "ae33_bc.png"
    if nfigs != 1:
        ax_2.set_title(title, loc='right')
        plotname = path_to_figures + name + '_2_day'
        fig.savefig(plotname + '.svg', facecolor=facecolor, bbox_inches='tight') 
        fig.savefig(plotname + '.png', facecolor=facecolor, bbox_inches='tight') 


    #####################################
    #####################################
    ## Make average by three points
    data = average_by_three(datum)
    if debug_mode:
        print(data.head(2))
    ## get only last two weeks
    xmin = data.index.max() - pd.to_timedelta("336:00:00") ## 14 days
    data = data[data.index >= xmin]

    ## set new axis label format
    fmt = dates.DateFormatter('%d-%2m-%Y')
    try:
        print(datetime.now().strftime(fmt))
    except:
        fmt = dates.DateFormatter('%d/%m/%Y')

    plt.rcParams['xtick.labelsize'] = 8
    if debug_mode:
        print(data.index.min(), data.index.max(), "delta:", data.index.max() - data.index.min())
    xlims = (data.index.min(), data.index.max() + pd.to_timedelta("4:00:00"))


    ##########################
    ## Figure 3: ['CH4', 'CO', 'NO', 'NO2', 'OZ', 'SO2']
    if nfigs == 1:
        ax_3 = fig.add_subplot(3, 2, 5)
        #ax_3 = fig.add_subplot(4, 2, 7)
    else:
        fig = plt.figure(figsize=(10, 5))
        ax_3 = fig.add_subplot(1, 1, 1)


    for i in range(len(columns)):
        wave = columns[i]   # 
        xx = data[wave].replace(np.nan, 0)
        #print(wave, xx.shape, all(xx.values), xx.values)
        if not any(xx.values):
            continue        

        xx = data[wave].replace(0, np.nan)
        if wave == 'CH4':
            xx = xx / 100
            ax_3.plot(xx.index, xx, label=wave + ' / 100') # color='red',
        elif wave == 'CO':
            xx = xx / 10
            ax_3.plot(xx.index, xx, label=wave + ' / 10')
        elif wave == 'SO2':
            xx = xx * 10
            ax_3.plot(xx.index, xx, label=wave + ' * 10')
        else:
            ax_3.plot(xx.index, xx, label=wave)

    #ax_3.set_xlim(left=xx.index.min())
    #ax_3.set_xlim(left=xmin)
    ax_3.set_xlim(xlims)
    ax_3.set_ylim(bottom=0)
    ax_3.legend() # ncol = 7, fontsize = 9)
    
    ax_3.tick_params(axis='x', labelrotation=labelrotation)
    
    ax_3.xaxis.set_major_formatter(fmt)
    ax_3.xaxis.set_minor_locator(locator)
    ax_3.grid(which='major', alpha=0.9)
    ax_3.grid(which='minor', alpha=0.5, linestyle='--')

    ## save to file "ae33_bc_waves_week.png"
    if nfigs != 1:
        ax_3.set_title(title, loc='right')
        plotname = path_to_figures + name + '_all_week'
        fig.savefig(plotname + '.png', facecolor=facecolor) #, bbox_inches='tight')
        #fig.savefig(path_to_figures + 'ae33_bc_waves_week.png', facecolor=facecolor) 


    ##########################
    ## Figure 4: PM10, PM2
    if nfigs == 1:
        ax_4 = fig.add_subplot(3, 2, 6)
    else:
        fig = plt.figure(figsize=(10, 5))
        ax_4 = fig.add_subplot(1, 1, 1)

    for wave in ['PM10', 'PM2.5']:
        yy = data[wave].replace(0, np.nan)
        if wave == 'PM10':
            color = 'dimgray'
        else:      
            color = 'mediumorchid'
        ax_4.plot(yy, color=color, label=wave)
        #ax_4.fill_between(yy.index, yy, np.zeros_like(yy), color=color)

    #ax_4.set_xlim(left=zz.index.min())
    ax_4.set_xlim(xlims)
    ax_4.set_ylim(bottom=0)
    ax_4.legend()
    
    ax_4.tick_params(axis='x', labelrotation=labelrotation)
    
    ax_4.xaxis.set_major_formatter(fmt)
    ax_4.xaxis.set_minor_locator(locator)
    ax_4.grid(which='major', alpha=0.9)
    ax_4.grid(which='minor', alpha=0.5, linestyle='--')

    ## save one figure to files
    if nfigs != 1:
        ax_4.set_title(title, loc='right')
        plotname = path_to_figures + name + '_2_week'
        #fig.savefig(plotname + '.svg', facecolor=facecolor, bbox_inches='tight') 
        fig.savefig(plotname + '.png', facecolor=facecolor) #, bbox_inches='tight') 


    #####################################
    ## save four plots to file
    if nfigs == 1:
        plotname = path_to_figures + name + '_four_plots.png'
        print(plotname)
        fig.savefig(plotname, 
                    facecolor=facecolor, # facecolor='lightgray',
                    #, bbox_inches = 'tight'
                   )



## --------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    debug_mode = False
    sep = get_folder_separator()
    dirname = "." + sep + "data" + sep
    path_to_figures = "." + sep + "figures" + sep
    
    timestamp = str(datetime.now())[:7].replace('-', '_')    #'2022_11'  #'2022_06'
    if debug_mode:
        print("timestamp:", timestamp)
    #filename = timestamp + '_mav_mos_mgu.xlsx'
    filename = timestamp + '_mav_mos_mgu.csv'
    xlsfilename = dirname + filename

    ## check data file
    if not os.path.exists(xlsfilename):
        #if debug_mode:
            print(xlsfilename, "is not found")
    else:
        if debug_mode:
            print(xlsfilename, "exists")

    ## check path to figures
    #print(path_to_figures)
    if not os.path.isdir(path_to_figures):
        os.makedirs(path_to_figures)


    ## read and prepare data
    datum = prepare_data(xlsfilename)
    if debug_mode:
        print(datum.head(2))


    # create four figures
    plot_four_figures_from_excel(datum, path_to_figures, 4, name='web_msu')

    # create one figure with four graphs
    plot_four_figures_from_excel(datum, path_to_figures, 1, name='web_msu')
