from pyspark.sql import SparkSession
from pyspark.sql import DataFrame as sparkDF
from pandas import Series
from pandas import DataFrame
from pandas import DatetimeIndex
from datastore import datastore as ds
import datetime
import pyspark.sql.functions as F
from processing import data


def distAVG(forecast):
    #create map for checking if ress is now dead
    res: DataFrame=data.getActiveResourcesOnDate(forecast.index[0])
    resources=res
    resultDF=DataFrame(columns=resources)
    if isinstance(forecast,Series):
        forecastDF: DataFrame=forecast.to_frame()
    else:
        forecastDF=forecast
    rowIter=forecastDF.iterrows()

    numres=len(resources)
    for index,value in rowIter:
        # for each row in the forcast
        row = { resources[i] : 0  for i in range(0, len(resources) ) }
        current_value=value.values[0]
      
        if current_value>=0:
            avarageValue=current_value/numres
            for res_id in resources:
        	    row[res_id]=avarageValue
            pass
        else:
            for res_id in resources:
        	    row[res_id]=0
            pass

            
        #row["index"]=index
        row["forecast"]=value.values[0]
        temp=DataFrame(row,index=DatetimeIndex(data={index}))
        resultDF=resultDF.append(temp)
        if current_value==0:
            break
    return resultDF
    pass