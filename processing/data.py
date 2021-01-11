from log import log
from pickle import STRING
from pandas._libs.tslibs import Timestamp

from pandas.core.indexes.datetimes import DatetimeIndex
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pandas as pd
from matplotlib import pyplot
from datetime import datetime, timedelta

import numpy as np

workingdf: DataFrame=0
trainingdf: DataFrame=0
testdf: DataFrame=0
trainingset: pd.DataFrame=0
testset: pd.DataFrame=0
sampleGlobalDateStart=""
sampleGlobalDateSplit=""
sampleGlobalDateStop=""

def clear():
    global sampleGlobalDateStart,sampleGlobalDateSplit,sampleGlobalDateStop,workingdf,trainingdf,testdf,trainingset,testset
    sampleGlobalDateSplit=""
    sampleGlobalDateStop=""
    sampleGlobalDateStart=""
    workingdf=0
    trainingdf=0
    testdf=0
    trainingset=0
    testset=0

def initWorkingDataFrame(working: DataFrame):
    global workingdf
    workingdf=working

def filterByBehaviour(behaviour):
    global workingdf
    workingdf=workingdf.filter(workingdf["behaviour"]==behaviour)

    log.log_data("behaviour",behaviour)
    pass

def filterByProduct(product):
    global workingdf
    workingdf=workingdf.filter(F.col("product").like(product))
    log.log_data("product_filter",product)
    pass

def splitByDate(sampleDateStart, sampleDateSplit, sampleDateStop):
    global workingdf,trainingdf,testdf,trainingset,testset,sampleGlobalDateStart,sampleGlobalDateSplit,sampleGlobalDateStop
    workingdf=workingdf.sort(workingdf.consumption_date.asc())
    sampleGlobalDateStart=sampleDateStart
    sampleGlobalDateSplit=sampleDateSplit
    sampleGlobalDateStop=sampleDateStop
    
    trainingdf=workingdf.filter(F.col("consumption_date").between(sampleDateStart,sampleDateSplit))
    testdf=workingdf.filter(F.col("consumption_date").between(sampleDateSplit,sampleDateStop)).filter(F.col("consumption_date")!=sampleDateSplit)
    
    reportDict=({ 'time_start':sampleDateStart, 'time_split': sampleDateSplit, 
            'time_stop':sampleDateStop})
    log.log_data("time_selection",reportDict)
    pass
def autoSplit():
    global workingdf,trainingdf,testdf,trainingset,testset,sampleGlobalDateStart,sampleGlobalDateSplit,sampleGlobalDateStop
    workingdf=workingdf.sort(workingdf.consumption_date.asc())
    tempdf=workingdf.groupBy("consumption_date").sum("billing_quantity")

    
    tempdf=tempdf.toPandas()
    tempdf.set_index('consumption_date',inplace=True)
    tempdf:pd.DataFrame=tempdf.resample("D").sum()

    
    maxenter=int(len(tempdf.index)/2)
    splitslice=maxenter
    maxval=tempdf.iloc[maxenter].values[0]
    maxexit=0
    for i in range(splitslice):
        current=tempdf.iloc[splitslice+i].values[0]
        if current>maxval:
           maxval=current
           maxenter=i+splitslice
           continue
        elif current<maxval:
            maxexit=i+splitslice-1
            break
    maxdif=int((maxexit-maxenter)/2)
    splitindex=maxdif+maxenter

    if maxval>tempdf.iloc[splitindex].values[0]:
        splitindex=maxenter
    la=len(tempdf.index)
    if splitindex>=len(tempdf.index):
        splitindex=int(len(tempdf.index)/2)

    splitdate=tempdf.index[splitindex].replace(hour=3)
    sampleGlobalDateStart=tempdf.index[0].strftime('%Y-%m-%d %H:%M:%S')
    sampleGlobalDateSplit=splitdate.strftime('%Y-%m-%d %H:%M:%S')
 
    sampleGlobalDateStop=(splitdate+timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')
    
    trainingdf=workingdf.filter(F.col("consumption_date").between(sampleGlobalDateStart,sampleGlobalDateSplit))
    testdf=workingdf.filter(F.col("consumption_date").between(sampleGlobalDateSplit,sampleGlobalDateStop)).filter(F.col("consumption_date")!=sampleGlobalDateSplit)
    
    reportDict=({ 'time_start':sampleGlobalDateStart, 'time_split': sampleGlobalDateSplit, 
            'time_stop':sampleGlobalDateStop})
    #log.log_date("time_selection",reportDict)
    pass


def realDateSplit(cluster: str):
    if cluster=="shortA":
        splitByDate("2020-08-05 00:00:00","2020-10-02 03:00:00","2020-11-02 03:00:00")
    elif cluster=="shortB":
        splitByDate("2020-08-05 00:00:00","2020-10-28 03:00:00","2020-11-28 03:00:00")
    elif cluster=="shortC":
        splitByDate("2020-08-05 00:00:00","2020-10-10 03:00:00","2020-11-10 03:00:00")
    elif cluster=="shortD":
        splitByDate("2020-08-05 00:00:00","2020-10-28 03:00:00","2020-11-28 03:00:00")
    elif cluster=="mediumA":
        splitByDate("2020-08-05 00:00:00","2020-10-12 03:00:00","2020-11-12 03:00:00")
    elif cluster=="mediumB":
        splitByDate("2020-11-01 00:00:00","2021-01-27 03:00:00","2021-02-27 03:00:00")
    elif cluster=="long":
        splitByDate("2020-09-19 00:00:00","2020-11-05 03:00:00","2020-12-05 03:00:00")
    # Cluster A
    elif cluster=="short":
        splitByDate("2020-08-05 00:00:00","2020-10-17 03:00:00","2020-11-17 03:00:00")
    elif cluster=="medium":
        splitByDate("2020-08-05 00:00:00","2020-11-01 03:00:00","2020-12-01 03:00:00")

def plotCurrentData():
    
    plottrainingset=trainingset.rename(columns={"sum(billing_quantity)":"training"})
    
    plotset=plottrainingset.append(testset.rename(columns={"sum(billing_quantity)":"test"}))
    plotset.plot()


def plotForcast(usedData,testData,modelFitted,forcast):
    
    plottrainingset=usedData.rename(columns={"sum(billing_quantity)":"training"})
    plotset=plottrainingset.append(testData.rename(columns={"sum(billing_quantity)":"test"}))
    #plotset=plotset.assign(fitted=modelFitted)
    plotset=plotset.assign(forecast=forcast)

    plotset.plot()
    pyplot.axvline(x=modelFitted.index[-1])

    pass

def buildSet():
    global workingdf,trainingdf,testdf,trainingset,testset,sampleGlobalDateStart,sampleGlobalDateSplit,sampleGlobalDateStop
    workingdf.show(10)
  
    trainingdf=trainingdf.groupBy("consumption_date").sum("billing_quantity")
    testdf=testdf.groupBy("consumption_date").sum("billing_quantity")
    trainingset=trainingdf.toPandas()
    testset=testdf.toPandas()
 
    trainingset.set_index('consumption_date',inplace=True)
    
    testset.set_index('consumption_date',inplace=True)
    trainRange=pd.date_range(start=datetime.strptime(sampleGlobalDateStart, '%Y-%m-%d %H:%M:%S'),end=datetime.strptime(sampleGlobalDateSplit, '%Y-%m-%d %H:%M:%S'))
    testRange=pd.date_range(start=datetime.strptime(sampleGlobalDateSplit.split(" ")[0], '%Y-%m-%d')+timedelta(days=1),end=datetime.strptime(sampleGlobalDateStop.split(" ")[0], '%Y-%m-%d'))
    #2020-9-01 00:00:00
    trainingset=trainingset.resample("D").sum().reindex(trainRange).fillna(0)
    testset=testset.resample("D").sum().reindex(testRange).fillna(0)
    print(testset.head(10))

def plotStats():
    bqlength=workingdf \
            .groupBy("resource_id") \
            .agg(F.sum("billing_quantity").alias("length")) \

    pbql=bqlength.toPandas()
    reportDict={}
    reportDict["cluster"]=pbql.describe().to_dict()
    subreport=({'train':trainingset.describe().to_dict(),'test':testset.describe().to_dict()})
    reportDict["split"]=subreport
    pbql.hist(bins=100,legend=True)
    log.log_data("Timeseries Stats",reportDict)





def convertAutoArimaForecastToPandas(forecast):
    forecastRange=pd.date_range(start=datetime.strptime(sampleGlobalDateSplit.split(" ")[0], '%Y-%m-%d')+timedelta(days=1),end=datetime.strptime(sampleGlobalDateStop.split(" ")[0], '%Y-%m-%d'))
    return pd.DataFrame(forecast,index=forecastRange)

def getActiveResourcesOnDate(date: Timestamp):

    end_date = date.to_pydatetime() - timedelta(days=1)
    ress=workingdf.filter(F.col("consumption_date").between(end_date,date.to_pydatetime())).select("resource_id").toPandas().squeeze()
    if isinstance(ress,str):
        return [ress]
    return ress.unique()

def getBillQuantFromResourceBetween(resource_id,start,stop):
    return workingdf.filter(F.col("consumption_date").between(start,stop)).filter(workingdf.resource_id==resource_id).select("consumption_date","billing_quantity").toPandas().set_index('consumption_date')