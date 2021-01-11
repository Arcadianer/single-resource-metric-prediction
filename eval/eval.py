
from pandas._libs.tslibs import Timestamp

from pandas.core.indexes.datetimes import DatetimeIndex
from pyspark.sql import DataFrame 
import pandas as pd

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from matplotlib import pyplot
from datetime import datetime, timedelta
import numpy as np
import processing.data as data
def forecast_accuracy(forecast: DataFrame, actual: DataFrame):
    forecast=forecast.to_numpy().squeeze()
    actual=actual[:len(forecast)].to_numpy().squeeze()
    return _numpy_forcast_accuracy(forecast,actual)

def _numpy_forcast_accuracy(forecast,actual):
    ae = np.sum(np.abs(forecast-actual))
    mape = np.mean(np.abs(forecast - actual)/np.abs(actual))  # MAPE
    me = np.mean(forecast - actual)             # ME
    mae = np.mean(np.abs(forecast - actual))    # MAE
    mpe = np.mean((forecast - actual)/actual)   # MPE
    rmse = np.mean((forecast - actual)**2)**.5  # RMSE
    corr = np.corrcoef(forecast, actual)[0,1]  # corr
            # minmax                  
    return({ 'ae':ae,'me':me, 'mae': mae, 
            'rmse':rmse,  
            'corr':corr})

def dist_accuracy(forecast: pd.DataFrame):
    resids=forecast.columns[0:-1]
    resultMap={}
    metricDF=pd.DataFrame()
    for value in resids:
        pred=forecast[value].to_numpy(dtype="float32")
        testRange=pd.date_range(start=forecast.index[0],end=forecast.index[-1])
        actual_temp=data.getBillQuantFromResourceBetween(value,forecast.index[0],forecast.index[-1]).resample("D").sum().reindex(testRange).fillna(0).squeeze()
        if (actual_temp is pd.Series):
            actual=actual_temp.to_numpy()
        else:
            actual=[actual_temp]
        temp=_numpy_forcast_accuracy(pred,actual)
        metricDF=metricDF.append(temp,ignore_index=True)
    
    metricsIDs=metricDF.columns[0:-1]
    for met in metricsIDs:
        resultMap[met]=np.mean(metricDF[met].tolist())
    return resultMap

def plot_dist_nice(forecast: pd.DataFrame):
    resids=forecast.columns
    name=[None]*resids.size
    id=0
    for value in range(resids.size):
        id=id+1
        name[value]="Server "+str(id) 
    name[-1]="forecast"
    forecast.columns=name
    forecast.plot()

    return

def plot_forcast_vs_actual(forecast: pd.DataFrame):
    resids=forecast.columns[0:-1]
    id=0
    for value in resids:
        id=id+1
        pred=forecast[value]
        testRange=pd.date_range(start=forecast.index[0],end=forecast.index[-1])
        pred.name="Server "+str(id)
        actual:pd.Series=data.getBillQuantFromResourceBetween(value,forecast.index[0],forecast.index[-1]).resample("D").sum().reindex(testRange).fillna(0)["billing_quantity"]
        actual.name="actual"
        print_df:pd.DataFrame= pd.concat([pred,actual],axis=1)

        print_df.plot(color=["b","r"])
    return
        
