from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.statespace.sarimax import SARIMAX, SARIMAXResults
from statsmodels.tsa.stattools import arma_order_select_ic
from statsmodels.tsa.statespace import tools
from statsmodels.graphics import tsaplots
from . import data
import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.tsa.stattools import adfuller
import pmdarima.arima as pm
from log import log
model: ARIMA
model_fit: SARIMAXResults=0


def fitWith(p: int ,d:int , q: int,S:int ,P:int ,D: int,Q: int ):  
    global model,model_fit
    model=SARIMAX(data.trainingset,order=(p,d,q),seasonal_order=(P,D,Q,S),trend="c")
    #model=ARIMA(data.trainingset,order=(p,d,q))
    model_fit = model.fit()
    print(model_fit.summary())
    reportDict=({'model_used':"sarima",'order':(p,d,q),'seasonal_order':(P,D,Q,S)})
    log.log_data("model",reportDict)

    pass
def autoArima(minp: int, minq: int,maxp: int,maxq: int):
    global model_fit
    #model_fit=pm.auto_arima(data.trainingset,
    #                  start_p=minp, start_q=minq,
    #                  min_p=minp, min_q=minq,
    #                  test='adf',       # use adftest to find optimal 'd'
    #                  max_p=maxp, max_q=maxq,
    #                  m=1,              # frequency of series
    #                  d=None,           # let model determine 'd'
    #                  trace=True,
    #                  error_action='ignore',  
    #                  suppress_warnings=True, 
    #                  stepwise=True
    # )
    sumset=data.trainingset.append(data.testset)

    model_fit=pm.auto_arima(sumset,out_of_sample_size=len(data.testset),m=1,trace=True,start_p=minp,min_q=minq,max_p=maxp,max_q=maxq,d=None,stepwise=False,max_order=None,return_valid_fits=True)
   
    current: ARIMA=model_fit[0]
    for mod in model_fit:
        p=mod.order[0]
        if p>=minp:
            current=mod
            break
        pass
    model_fit=current
    print(model_fit.summary())
    pass

def ACFPlot(diffi: int,seasonal_diff: int ,season_period: int):
    reportDict=({'d':diffi,'s':seasonal_diff,'sp':season_period})
    result = adfuller(data.trainingset[["sum(billing_quantity)"]], autolag='AIC')
    print(f'ADF Statistic: {result[0]}')
    reportDict["ADF Statistic"]=result[0]
    reportDict["p-value"]=result[1]
    print(f'p-value: {result[1]}')
    for key, value in result[4].items():
        print('Critial Values:')
        print(f'   {key}, {value}')
    fig, ax = plt.subplots(1,2,figsize=(10,5))
    lag=9#(len(data.trainingset[["sum(billing_quantity)"]])/2)-2
    tsaplots.plot_acf(tools.diff(data.trainingset[["sum(billing_quantity)"]],diffi,seasonal_diff,season_period),ax=ax[0])
    tsaplots.plot_pacf(tools.diff(data.trainingset[["sum(billing_quantity)"]],diffi,seasonal_diff,season_period),lags=lag,ax=ax[1])
    log.log_data("acf",reportDict)

0
def modelSelector(behaviour: str):
    sleect={}
    sleect["shortA"]=(1,0,0,20,0,0,0)
    sleect["shortB"]=(2,0,0,20,0,0,0)
    sleect["shortC"]=(2,1,1,20,0,0,0)
    sleect["shortD"]=(1,1,1,15,0,0,1)
    sleect["mediumA"]=(1,1,1,20,0,0,1)
    sleect["mediumB"]=(1,1,1,20,0,0,1)
    sleect["long"]=(0,1,1,20,0,0,0)
    sleect["short"]=(2,0,0,20,0,0,0)
    sleect["medium"]=(2,0,1,20,0,0,0)
    return sleect[behaviour]