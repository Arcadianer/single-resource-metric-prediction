from operator import truediv
from log import log
import pyspark
from pyspark.sql import SparkSession
from datastore import datastore as ds
import datetime
import pyspark.sql.functions as F

from pyspark.sql.types import IntegerType, StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from sklearn.cluster import KMeans
import pandas as pd
import matplotlib

tag_min=0
tag_max=0
tag_name=0
_reportDict={}

def generateBehaviourTagMinMax():
    
    generateBehaviourTagByLength(0,1,"shortA")
    generateBehaviourTagByLength(1,4,"shortB")
    generateBehaviourTagByLength(4,9,"shortC")
    generateBehaviourTagByLength(9,14,"shortD")
    generateBehaviourTagByLength(15,19,"mediumA")
    generateBehaviourTagByLength(20,29,"mediumB")
    generateBehaviourTagByLength(30,365,"long")
    joinGenBehaviour()
    log.log_data("cluster_definitions",_reportDict)

def generateBehaviourTagMinMaxClusterA():
    
    generateBehaviourTagByLength(0,5,"short")
    generateBehaviourTagByLength(5,20,"medium")
    generateBehaviourTagByLength(20,360,"long")

    joinGenBehaviour()
    log.log_data("cluster_definitions",_reportDict)

def generateBehaviourTagByLength(min,max,name):
    global maxDict,minDict
    initLengthDF()
    tag_min=min*48
    tag_max=max*48
    tag_name=name
    ds.maxDict[tag_name]=tag_max
    ds.minDict[tag_name]=tag_min
    _reportDict[tag_name]="["+str(tag_min)+","+str(tag_max)+"]"
    tempdf=ds.lengthdf.filter((ds.lengthdf["length"]>=tag_min) & (ds.lengthdf["length"]<=tag_max) ).withColumn("gen_behaviour",F.lit(name))


    if ds.genbehaviourdf==0:
        ds.genbehaviourdf=tempdf
    else:
       ds.genbehaviourdf=ds.genbehaviourdf.union(tempdf)


def initLengthDF():
  
    if ds.lengthdf==0:

        bqlength:ds.DataFrame=ds.rootdf \
            .where(ds.rootdf.product.like("OTC_S%")) \
            .groupBy("resource_id") \
            .agg(F.sum("billing_quantity").alias("length")) \
       

        minleng=ds.rootdf \
            .where(ds.rootdf.product.like("OTC_S%")) \
            .groupBy("resource_id") \
            .agg({"consumption_date":"min"}) \
  
        maxleng=ds.rootdf \
            .where(ds.rootdf.product.like("OTC_S%")) \
            .groupBy("resource_id") \
            .agg({"consumption_date":"max"}) \
            
        minmaxdf=minleng.join(maxleng,minleng.resource_id==maxleng.resource_id,'inner') \
            .drop(maxleng.resource_id) 
   

        #ds.lengthdf=minmaxdf.withColumn("length",(((F.unix_timestamp(minmaxdf[2])-F.unix_timestamp(minmaxdf[0]))/86400)+1).cast(IntegerType())) \
        #    .select("resource_id","length")
        ds.lengthdf=bqlength.select("resource_id","length")
       
       
def mapBehaviour(x):
    if x.Length>tag_min and x.Length<tag_max:
        return (x.resource_id,tag_name)
    #return (x.resource_id,"peter")

def joinGenBehaviour():
    
    gen=ds.genbehaviourdf.alias("gen")
    root=ds.rootdf.alias("root")
    ds.rootdf=root.join(gen,F.col("gen.resource_id")==F.col("root.resource_id")).drop(F.col("gen.resource_id"))

def generateBehaviourTagKMean():
    initLengthDF()
    lenghthdata=ds.lengthdf.toPandas()
    lenghthdata.dropna(inplace=True)
    sub=lenghthdata["length"].hist(bins=100)
    
    fitdata=lenghthdata.copy()
    fitdata["resource_id"]=1

    kmean=KMeans().fit(fitdata)
    lables=kmean.predict(fitdata)
    lenghthdata["gen_behaviour"]=lables
    
    stats=_createKmeanStats(kmean,lenghthdata)
    inera=kmean.inertia_
    stats["ssd"]=kmean.inertia_
    ds.genbehaviourdf=ds.spark.createDataFrame(lenghthdata)
    
    for i in stats:
        if i!="ssd":
            min=stats[i]["min"]
            sub.vlines(x=min,ymin=0,ymax=1)
            max=stats[i]["max"]
            sub.vlines(x=max,ymin=0,ymax=1)

    kmeanrep=({'kmean-para':kmean.get_params(deep=True),'kmean-stats':stats})
    log.log_data("kmean",kmeanrep)
    joinGenBehaviour()



def _createKmeanStats(kmean,lenghthdata):
    min={}
    max={}
    returnDict={}
    for index, row in lenghthdata.iterrows():
        beh=row['gen_behaviour']
        if beh not in min:
            min[beh]=row["length"]
        if beh not in max:
            max[beh]=row["length"]
        if min[beh]>row["length"]:
            min[beh]=row["length"]
        if max[beh]<row["length"]:
            max[beh]=row["length"]
    for i in range(len(kmean.cluster_centers_)):
        returnDict[i]=({"id":i,"center":kmean.cluster_centers_[i],"min":min[i],"max":max[i]})
    return returnDict
    

