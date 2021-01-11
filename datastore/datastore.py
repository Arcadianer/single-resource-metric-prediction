from datetime import datetime, timedelta
from pyspark.sql import DataFrame
from matplotlib import pyplot
import pandas as pd 
from pyspark.sql import SparkSession
import math
import random
import uuid

spark: SparkSession=0
rootdf: DataFrame=0
lengthdf: DataFrame=0
genbehaviourdf: DataFrame=0
# DF preped with all nessesary data
masterdf: DataFrame=0
_behaviourName=""
maxDict={}
minDict={}

_seed=0

def clear():
    global spark,rootdf,lengthdf,genbehaviourdf,masterdf,_behaviourName,maxDict,minDict
    spark.stop()
    spark=0
    rootdf=0
    lengthdf=0
    genbehaviourdf=0
    # DF preped with all nessesary data
    masterdf=0
    _behaviourName=""
    maxDict={}
    minDict={}
    


def generateDatasetWithBehaviour(seed,behaviour: str,startdate: datetime,length: int):
    global _behaviourName
    _behaviourName=behaviour
    random.seed(seed)
    print("Generating Dataset....")
    global masterdf
    bdata=0
    if(behaviour=="shortA"):
        bdata=_generateShortA(length)
    elif(behaviour=="short"):
        bdata=_generateShort(length)
    elif(behaviour=="shortB"):
        bdata=_generateshortB(length)
    elif(behaviour=="shortC"):
        bdata=_generateshortC(length)
    elif(behaviour=="shortD"):
        bdata=_generateshortD(length)
    elif(behaviour=="mediumA"):
        bdata=_generateMedium1(length)
    elif(behaviour=="medium"):
        bdata=_generateMedium(length)
    elif(behaviour=="mediumB"):
        bdata=_generateMedium2(length)
    elif(behaviour=="short-medium"):
        bdata=_generateShortMiddle(length)
    elif(behaviour=="long"):
        bdata=_generateLong(length)
    df=_buildMasterDF(bdata,startdate,"OTC_S1",behaviour)
    masterdf=spark.createDataFrame(df)
    print("Dataset done!")
    pass

def _generateShortA(length: int):
    #########
    minLife=0
    maxLife=48
    bqPerDay=48
    minServerSpawn=0
    maxServerSpawn=3
    minTimeServerSpawn=3
    maxTimeServerSpawn=14
    ########
    return _generateBehaviour(minLife,maxLife,bqPerDay,minServerSpawn,maxServerSpawn,minTimeServerSpawn,maxTimeServerSpawn,length)

def _generateShort(length: int):
    #########
    minLife=0
    maxLife=240
    bqPerDay=48
    minServerSpawn=0
    maxServerSpawn=3
    minTimeServerSpawn=3
    maxTimeServerSpawn=14
    ########
    return _generateBehaviour(minLife,maxLife,bqPerDay,minServerSpawn,maxServerSpawn,minTimeServerSpawn,maxTimeServerSpawn,length)

def _generateshortB(length: int):
    #########
    minLife=48
    maxLife=192
    bqPerDay=48
    minServerSpawn=0
    maxServerSpawn=4
    minTimeServerSpawn=7
    maxTimeServerSpawn=20
    ########
    return _generateBehaviour(minLife,maxLife,bqPerDay,minServerSpawn,maxServerSpawn,minTimeServerSpawn,maxTimeServerSpawn,length)

def _generateshortC(length: int):
    #########
    minLife=192
    maxLife=432
    bqPerDay=48
    minServerSpawn=0
    maxServerSpawn=5
    minTimeServerSpawn=7
    maxTimeServerSpawn=35
    ########
    return _generateBehaviour(minLife,maxLife,bqPerDay,minServerSpawn,maxServerSpawn,minTimeServerSpawn,maxTimeServerSpawn,length)

def _generateshortD(length: int):
    #########
    minLife=432
    maxLife=672
    bqPerDay=48
    minServerSpawn=0
    maxServerSpawn=5
    minTimeServerSpawn=20
    maxTimeServerSpawn=60
    ########
    return _generateBehaviour(minLife,maxLife,bqPerDay,minServerSpawn,maxServerSpawn,minTimeServerSpawn,maxTimeServerSpawn,length)

def _generateMedium1(length: int):
    #########
    minLife=720
    maxLife=820
    bqPerDay=48
    minServerSpawn=0
    maxServerSpawn=4
    minTimeServerSpawn=25
    maxTimeServerSpawn=60
    ########
    return _generateBehaviour(minLife,maxLife,bqPerDay,minServerSpawn,maxServerSpawn,minTimeServerSpawn,maxTimeServerSpawn,length)

def _generateMedium(length: int):
    #########
    minLife=240
    maxLife=960
    bqPerDay=48
    minServerSpawn=0
    maxServerSpawn=4
    minTimeServerSpawn=7
    maxTimeServerSpawn=60
    ########
    return _generateBehaviour(minLife,maxLife,bqPerDay,minServerSpawn,maxServerSpawn,minTimeServerSpawn,maxTimeServerSpawn,length)

def _generateMedium2(length: int):
    #########
    minLife=960
    maxLife=2392
    bqPerDay=48
    minServerSpawn=0
    maxServerSpawn=4
    minTimeServerSpawn=30
    maxTimeServerSpawn=70
    ########
    return _generateBehaviour(minLife,maxLife,bqPerDay,minServerSpawn,maxServerSpawn,minTimeServerSpawn,maxTimeServerSpawn,length)


def _generateLong(length: int):
    #########
    minLife=1392
    maxLife=3780
    bqPerDay=48
    minServerSpawn=1
    maxServerSpawn=2
    minTimeServerSpawn=20
    maxTimeServerSpawn=90
    ########
    return _generateBehaviour(minLife,maxLife,bqPerDay,minServerSpawn,maxServerSpawn,minTimeServerSpawn,maxTimeServerSpawn,length)


def _generateShortMiddle(length: int):
    #########
    minLife=168
    maxLife=588
    bqPerDay=48
    minServerSpawn=0
    maxServerSpawn=5
    minTimeServerSpawn=5
    maxTimeServerSpawn=10
    ########
    return _generateBehaviour(minLife,maxLife,bqPerDay,minServerSpawn,maxServerSpawn,minTimeServerSpawn,maxTimeServerSpawn,length)





def _generateBehaviour(minLife: int,maxLife: int, bqPerDay: int, minServerSpawn: int ,maxServerSpawn: int,minTimeServerSpawn: int,maxTimeServerSpawn: int,datasetlength: int):
    global maxDict,minDict
    rows=pd.DataFrame()
    activeIDMap={}
    spawnTimer=0
    maxDict[_behaviourName]=maxLife
    minDict[_behaviourName]=minLife
    ##################

    for currentIndex in range(datasetlength):

        #Server Spawn
        prob=_retProb(minTimeServerSpawn,maxTimeServerSpawn,spawnTimer)
        serverCount=random.randint(minServerSpawn,maxServerSpawn)
        inst=_spawnInstances(prob,serverCount)
        if(len(inst)>0):
            spawnTimer=0
            for id in inst:
                activeIDMap[id]=0
                newRow[id]=random.randint(1,48)
            pass
        else: 
            spawnTimer=spawnTimer+1
        #Simulate current active
        newRow={}
        tempactive=list(activeIDMap)
        for id in tempactive:
            bq=activeIDMap[id]
            prob=_retProb(minLife,maxLife,bq)
            if(_runRandomExperiment(prob)):
                activeIDMap.pop(id,None) 
                # add decay
                if id in newRow:
                    diff=48-newRow[id]
                else:
                    diff=48
                newRow[id]=random.randint(1,diff)
            else:
                activeIDMap[id]=bq+bqPerDay
                newRow[id]=bqPerDay
        rows=rows.append(newRow,ignore_index=True)
    return rows

def _buildMasterDF(bdata: pd.DataFrame,dateStart: datetime,product: str,behaviour:str):
    prep=pd.DataFrame(columns=["product","resource_id","behaviour","consumption_date","billing_quantity"])
    index=pd.date_range(start=dateStart,end=dateStart+timedelta(days=len(bdata.index)-1))
    bdata.index=index
    for index,row in bdata.iterrows():
        for resID,bq in row.items():
            if not (math.isnan(bq)):
                con={"product":product,"resource_id":resID,"behaviour": behaviour,"consumption_date":index,"billing_quantity":bq}
                prep=prep.append(con,ignore_index=True)
    
    return prep    

def _spawnInstances(probability: float,count: int):
        if(_runRandomExperiment(probability)):
            #generate
            idList=[]
            for i in range(count):
                idList.append(str(uuid.uuid4()))
            return idList
        else:
            return []
def _runRandomExperiment(probability: float):
    select=random.randint(1,100)
    probability=probability*100
    if(select<probability):
        return True
    else:
        return False

def _retProb(min: int,max: int,current: int):
    # move along x
    minmax=abs((min-max))
    a=12/minmax
    x=(a*(current-min))-6
    return _sigmoid(x)


    
def _sigmoid(x):
    if x< (-100):
        return 0.0
    elif x>100:
        return 1.0
    return 1 / (1 + math.exp(-x))
