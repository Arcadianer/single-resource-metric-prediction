from datetime import datetime
import json
from processing import data
from matplotlib import pyplot , figure
from shutil import copyfile
import os

import matplotlib

report={}

def log_Experiment(logbasedir: str,name: str):
    now = datetime.now() # current date and time
    
    date_time = now.strftime("%y-%m-%d-%H-%M-%S")
    log_data("time",date_time)
    currentLogDir=logbasedir+name+"-"+date_time+"\\"
    os.mkdir(os.getcwd()+"\\"+currentLogDir)
    #Save png
    #matplotlib.use("png")

    #for i in pyplot.get_fignums():
    #    pyplot.figure(i)
    #    #matplotlib.use(curr)
    #    pyplot.savefig(currentLogDir+'figure%d.png' % i)
    
    
    for i in pyplot.get_fignums():
        pyplot.figure(i)

        #matplotlib.use("pgf")
        #pyplot.savefig(currentLogDir+'figure%d.pgf' % i)
        #pyplot.figure(i)
        
        pyplot.savefig(currentLogDir+'figure%d.png' % i,dpi=1000)
   
    #Copy main.py
    copyfile("main.py",  currentLogDir+"main.py")

   
    strreport=_render_report(report,"==== Experiment Log ====",0)

    f = open(currentLogDir+"report.md", "w")
    f.write(strreport)
    f.close()
    # write to big file
    f = open("db.json","a+")
    pref=f.read()
    asa=pref+json.dumps(report)+"\n"
    f.write(asa)
    f.close()
    pass

def _render_report(item,topic,depth):
    renderString=""

    depth=depth+1
    heading=""
    for temp in range(depth):
        heading=heading+"#"
    heading=heading+" "+str(topic)+"\n"


    if depth==1:
        now = datetime.now() # current date and time
        date_time = now.strftime("%y-%m-%d-%H-%M-%S")
        renderString=renderString+"This experiment was conducted on "+report["time"]+"\n\n"
    dataString=""
    if type(item) is dict: 
        for tp,val in item.items():
            dataString=dataString+"\n"+ _render_report(val,tp,depth)+"\n"
        renderString=renderString+heading+dataString
    elif type(item) is list:
        for entry in item:
            dataString=dataString+entry+"\n"
        renderString=renderString+heading+dataString
    else:
        dataString=dataString+topic+"="+str(item)+"\n"
        renderString=renderString+dataString

    return renderString


def log_data(topic,x):
    report[topic]=x
    print(topic+"="+str(x))