from pyspark.sql import SparkSession
from datastore import datastore as ds
from pyspark.sql import DataFrame

workingmaster: DataFrame=0

def clear():
    global workingmaster
    workingmaster=0

def masterPrep():
    setupWorkingMaster()
    mergeBehaviour()
    reduceColumns()
    sortByDate()
    build()
 
    pass

def mergeBehaviour():
    global workingmaster
    if hasColumn(workingmaster,"behaviour"):
        workingmaster=workingmaster.withColumn("behaviour",workingmaster.coalesce(workingmaster.behaviour,workingmaster.gen_behaviour))
    else:
        workingmaster=workingmaster.withColumnRenamed("gen_behaviour","behaviour")


def hasColumn(df, path):
    return path in df.columns

def reduceColumns():
    global workingmaster
    workingmaster=workingmaster.select("product","resource_id","behaviour","consumption_date","billing_quantity")
def sortByDate():
    global workingmaster
    workingmaster=workingmaster.orderBy("consumption_date",ascending=False)
def setupWorkingMaster():
    global workingmaster
    if workingmaster==0:
        workingmaster=ds.rootdf
def build():
    ds.masterdf=workingmaster