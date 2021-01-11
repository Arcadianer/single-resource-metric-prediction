from pyspark.sql import SparkSession
from datastore import datastore 

def loadFromDB(url , user , password , table):
    initSpark()
    datastore.rootdf=datastore.spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", table) \
        .option("user", user) \
        .option("password",password) \
        .load()

def masterToDB(url , user , password , table,overwrite):
    creds=({"user":user,"password":password})
    if overwrite==True:
        datastore.masterdf.write.jdbc(url,table,mode="overwrite",properties=creds)
    else:
        datastore.masterdf.write.jdbc(url,table,mode="append",properties=creds)
    pass
def filterProduct(product):
    datastore.rootdf=datastore.rootdf.where(datastore.rootdf.product.like(product))

def initSpark():
    if datastore.spark==0 :
        datastore.spark=SparkSession.builder.appName("ot3c-analytics").getOrCreate()