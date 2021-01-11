import datastore
from preprocessing.masterPrep import masterPrep
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import pandas
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from statsmodels.graphics.factorplots import interaction_plot
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.stattools import acf
from pandas import DataFrame
from matplotlib import pyplot
from datastore import datastore as ds
import preprocessing.loaddata as loaddata
import preprocessing.generateTagData as gendata
import preprocessing.masterPrep as mp
import processing.data as data
import processing.arima as arima
import processing.deepar as deepar
import postprocessing.averageDistribution as avgdist
import postprocessing.simpleDistribution as simp
from eval import eval
from log import log
from datetime import datetime
from playsound import playsound
import shutil



#INIT
####################
clustergroup="clusterB"
behaviour="shortD"
model="D"
distAlgo="C"
dataOrigin="G0"

run=0

def clear():
    ds.clear()
    data.clear()
    mp.clear()
    plt.close("all")
    shutil.rmtree('C:\\Users\\handg\\.matplotlib',ignore_errors=True)

def experiment():
    global clustergroup,behaviour,model,distAlgo,dataOrigin,run
    ######## Logging
    experimentPrelid=({"clusterGroup":clustergroup,"cluster":behaviour,"dataOrigin":dataOrigin,"model":model,"dist":distAlgo})
    log.log_data("Experiment Config",experimentPrelid)


    ####################
    loaddata.initSpark()

    if dataOrigin=="R0":
        #input("Press Enter to start analysis...")
        loaddata.loadFromDB("jdbc:postgresql://192.168.178.66:5432/ot3c","postgres","ot3c","consumptions")
        loaddata.filterProduct("OTC_S%")
        if clustergroup=="clusterB":
            gendata.generateBehaviourTagMinMax()
        else:
            gendata.generateBehaviourTagMinMaxClusterA()
        #gendata.generateBehaviourTagKMean()
        mp.masterPrep()
    else:
        ds.generateDatasetWithBehaviour(dataOrigin,behaviour,datetime.strptime("2020-08-05 00:00:00", '%Y-%m-%d %H:%M:%S'),365)

    avgdist.buildAvgBillQuantPerBehaviour()

    #loaddata.masterToDB("jdbc:postgresql://192.168.178.66:5432/ot3c","postgres","ot3c",dataOrigin,True)

    data.initWorkingDataFrame(ds.masterdf)
    data.filterByBehaviour(behaviour)
    if dataOrigin!="R0":
        data.autoSplit()
        #data.splitByDate("2020-08-05 00:00:00","2020-10-27 03:00:00","2021-02-15 03:00:00")
        pass
    else:
        data.realDateSplit(behaviour)
        
    data.buildSet()
    data.plotCurrentData()
   
    step=len(data.testset.index)

    ## ARIMA
    arima.ACFPlot(1,0,9)
    data.plotStats()
    #plt.show()
    #### AUTO
    #arima.autoArima(1,0,15,2)

    #forecast=arima.model_fit.predict(n_periods=step)
    #forecast=data.convertAutoArimaForecastToPandas(forecast)
    #### Manual
    if model=="A":
        s=arima.modelSelector(behaviour)
        arima.fitWith(s[0],s[1],s[2],s[3],s[4],s[5],s[6])
        forecast=arima.model_fit.forecast(steps=30)
        arima.model_fit.plot_diagnostics(figsize=(7,5))

    ## DEEPAR
    elif model=="D":
        deepar.fitDeepARNegativeBinomial(30,7,7)
        #log.log_data()
        #forcast_eva=deepar.evaluate()
        log.log_data("deep-eval",deepar.evaluate())
        forecast=deepar.forecast(data.trainingset)

    #step=7



    data.plotForcast(data.trainingset,data.testset,data.trainingset,forecast)
    forcast_acc=eval.forecast_accuracy(forecast,data.testset)

    if distAlgo=="C":
        dist:pandas.DataFrame=avgdist.distCFD(forecast,48,True,ds.maxDict[behaviour])
    else:
        dist:pandas.DataFrame=simp.distAVG(forecast)
    #dist.plot()
    eval.plot_dist_nice(dist)
    result=eval.dist_accuracy(dist)
    eval.plot_forcast_vs_actual(dist)

    log.log_data("forecast-length",len(forecast.index))
    log.log_data("count-dist-ressource",len(dist.columns[0:-1]))
    print("### REPORT ###")
    print("Forecast ACC")
    log.log_data("forcast_acc",forcast_acc)
    print("Dist ACC")
    log.log_data("dist_acc",result)
    logname=clustergroup+"_"+behaviour+"_"+dataOrigin+"_"+model+"_"+distAlgo
    
    log.log_Experiment("exp-log\\",logname)
    #playsound('done.mp3')
    plt.show()
    print("DONE")
 
    #input("Press Enter to continue...")
    clear()
    

def loadgenerator(overwrite):
    loaddata.initSpark()
    ds.generateDatasetWithBehaviour(dataOrigin,behaviour,datetime.strptime("2020-08-05 00:00:00", '%Y-%m-%d %H:%M:%S'),365)
    loaddata.masterToDB("jdbc:postgresql://192.168.178.66:5432/ot3c","postgres","ot3c",dataOrigin,overwrite)

#loadgenerator(True)

experiment()