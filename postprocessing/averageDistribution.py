from pyspark.sql import SparkSession
from pyspark.sql import DataFrame as sparkDF
from pandas import Series
from pandas import DataFrame
from pandas import DatetimeIndex
from datastore import datastore as ds
import datetime
import pyspark.sql.functions as F
from datastore import datastore as ds
from processing import data
import json

avgBillQuantDF=0


def buildAvgBillQuantPerBehaviour():
    global avgBillQuantDF
    temp=ds.masterdf.groupBy("behaviour","resource_id").sum("billing_quantity")
    temp=temp.groupBy("behaviour").avg("sum(billing_quantity)").collect()
    avgBillQuantDF={ i[0]: i[1]  for i in temp  }
    print(json.dumps(avgBillQuantDF, indent=4))
    pass

#returns negative numbers for bq left and positive for over avg
def diffFromAvgBillQuantByBehaviour(resource_id,selectionDate):
    res=ds.masterdf.filter(ds.masterdf.resource_id==resource_id).filter(ds.masterdf.consumption_date<=selectionDate).groupBy("behaviour").sum("billing_quantity").collect()
    pastBQ=res[0]["sum(billing_quantity)"]
    if pastBQ==None:
        pastBQ=0
    behaviour=res[0]["behaviour"]
    avg=avgBillQuantDF[behaviour]
    return pastBQ-avg

def distCFD(forecast: Series, maxBillQuantPerDay, canDie: bool,maxbq: int):
    res: DataFrame=data.getActiveResourcesOnDate(forecast.index[0])
    resources=res
    #create map for checking if ress is now dead
    deathMap = { resources[i] :  False  for i in range(0, len(resources) ) }
    currentBQMap = { resources[i] : diffFromAvgBillQuantByBehaviour(resources[i] ,forecast.index[0])  for i in range(0, len(resources) ) }
    # get current avg bq levels
    resultDF=DataFrame(columns=resources)
    if isinstance(forecast,Series):
        forecastDF: DataFrame=forecast.to_frame()
    else:
        forecastDF=forecast
    rowIter=forecastDF.iterrows()

    for index,value in rowIter:
        # for each row in the forcast
        sort=sorted(resources, key=lambda res: currentBQMap[res])
        row = { resources[i] : 0  for i in range(0, len(resources) ) }
        current_value=value.values[0]
        if current_value>=0:
            for res_id in sort:
                if currentBQMap[res_id]>= maxbq:
                    deathMap[res_id]=True
                if canDie and deathMap[res_id]:
                    row[res_id]=0
                    continue
                if current_value<maxBillQuantPerDay:
                    row[res_id]=current_value
                    currentBQMap[res_id]=currentBQMap[res_id]+current_value
                    if current_value==0:
                        deathMap[res_id]=True
                    current_value=0
                else:
                    row[res_id]=maxBillQuantPerDay
                    currentBQMap[res_id]=currentBQMap[res_id]+maxBillQuantPerDay
                    current_value=current_value-maxBillQuantPerDay
            pass
        else:
            for res_id in sort:
                    deathMap[res_id]=True
                    row[res_id]=0
            pass     
            
            
        #row["index"]=index
        row["forecast"]=value.values[0]
        temp=DataFrame(row,index=DatetimeIndex(data={index}))
        resultDF=resultDF.append(temp)
    return resultDF
    pass

  
