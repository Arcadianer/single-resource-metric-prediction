import main

datalist=("G0","G1","G2","G3")
modellist=("D","A")
distlist=("C","A")
clusterA=("short","medium","long")
clusterB=("shortA","shortB","shortC","shortD","mediumA","mediumB","long")
cluster=("clusterA,clusterB")

run=0

def configurationGenerator(clusterarray,modelarray,distarray,dataarray):
    indexdata=run%4
    indexdist=int(run/4)%2
    indexmodel=int(run/8)%2
    indexcluster=int(run/16)%7
    main.behaviour=clusterarray[indexcluster]
    main.model=modelarray[indexmodel]
    main.distAlgo=distarray[indexdist]
    main.dataOrigin=dataarray[indexdata]

def deeprconfigurationGenerator(clusterarray,modelarray,distarray,dataarray):
    indexdata=run%4
    indexdist=int(run/4)%2
    indexmodel=0
    indexcluster=int(run/8)%7
    main.clustergroup="clusterB"
    main.behaviour=clusterarray[indexcluster]
    main.model=modelarray[indexmodel]
    main.distAlgo=distarray[indexdist]
    main.dataOrigin=dataarray[indexdata]

def arimaconfigurationGenerator(clusterarray,distarray,dataarray):
    indexdata=run%4
    indexdist=int(run/4)%2
    indexmodel=0
    indexcluster=int(run/8)%7
    main.behaviour=clusterarray[indexcluster]
    main.model="A"
    main.distAlgo=distarray[indexdist]
    main.dataOrigin=dataarray[indexdata]

def realClusterBconfigurationGenerator(clusterarray,distarray,dataarray):
    indexdata=0
    indexdist=run%2
    indexmodel=int(run/2)%2
    indexcluster=int(run/4)%7
    main.behaviour=clusterarray[indexcluster]
    main.model=modellist[indexmodel]
    main.distAlgo=distarray[indexdist]
    main.dataOrigin="R0"

def realClusterAconfigurationGenerator(clusterarray,distarray,dataarray):
    indexdata=0
    indexdist=run%2
    indexmodel=int(run/2)%2
    indexcluster=int(run/4)%3
    main.behaviour=clusterarray[indexcluster]
    main.model=modellist[indexmodel]
    main.distAlgo=distarray[indexdist]
    main.dataOrigin=()

def deepargenAuto():
    global run
    runstart=26
    runmax=56
    diff=runmax-runstart
    for i in range(diff):
        run=i+runstart
        deeprconfigurationGenerator(clusterB,("D"),distlist,datalist)
        print("\n\n\n\n================= RUN "+str(run)+" =================")
        main.experiment()

def arimagenAuto():
    global run
    runstart=48
    runmax=56
    diff=runmax-runstart
    for i in range(diff):
        run=i+runstart
        arimaconfigurationGenerator(clusterB,distlist,datalist)
        print("\n\n\n\n================= RUN "+str(run)+" =================")
        main.experiment()

def clusterAConfigAutoGen():
    indexdata=run%4
    indexdist=int(run/4)%2
    indexmodel=int(run/8)%2
    indexcluster=int(run/16)%3
    main.behaviour=clusterA[indexcluster]
    main.model=modellist[indexmodel]
    main.distAlgo=distlist[indexdist]
    main.dataOrigin=datalist[indexdata]
    pass


def clusterAAuto():
    global run
    main.clustergroup="clusterA"
    runstart=0
    runmax=48
    diff=runmax-runstart
    for i in range(diff):
        run=i+runstart
        clusterAConfigAutoGen()
        print("\n\n\n\n================= RUN "+str(run)+" =================")
        main.experiment()

def realAuto():
    global run
    main.clustergroup="clusterA"
    runstart=0
    runmax=12
    diff=runmax-runstart
    for i in range(diff):
        run=i+runstart
        realClusterAconfigurationGenerator(clusterA,distlist,datalist)
        print("\n\n\n\n================= RUN "+str(run)+" =================")
        main.experiment()

    runstart=0
    runmax=28
    main.clustergroup="clusterB"
    diff=runmax-runstart
    for i in range(diff):
        run=i+runstart
        realClusterBconfigurationGenerator(clusterB,distlist,datalist)
        print("\n\n\n\n================= RUN "+str(run)+" =================")
        main.experiment()

def GenAutoGenConfig():
    indexdata=run%2
    indexCluster=int(run/2)%9
    beh=("shortA","shortB","shortC","shortD","mediumA","mediumB","long","short","medium")
    main.behaviour=beh[indexCluster]
    if(indexCluster>7):
        main.clustergroup="clusterA"
    else:
        main.clustergroup="clusterB"
    das=("G0","G1")
    main.dataOrigin=das[indexdata]

    pass

def saveGenerated():
    global run
    runstart=0
    runmax=16
    diff=runmax-runstart
    for i in range(diff):
        run=i+runstart
        GenAutoGenConfig()
        print("\n\n\n\n================= RUN "+str(run)+" =================")

        main.loadgenerator((run==0))

saveGenerated()