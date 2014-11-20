import csv
import rdflib
import urllib
import ntpath
import os
import tarfile
import requests
import rdfextras

from os import listdir
from os.path import isfile, join
from rdflib.serializer import Serializer
from rdflib import plugin

# Functions
def identifySerialisation( fileName ):
    #add all serialisations
    if fileName.endswith(".ttl"):
        return "turtle"
    else:
        return
        
def loadMetricConfiguration():    
    g = rdflib.Graph();
    config = g.parse("config.ttl", format="turtle")
    return g.serialize(format="json-ld", indent=0)
    
def formatMetricConfiguration(configStr):
    formattedStr = configStr.replace('\n', ' ').replace('\r', '').replace('"','\"')
    return formattedStr

# Main
with open('voidlist.csv', 'rb') as csvfile:
    voidreader = csv.reader(csvfile, delimiter=",")
    metricsConf = formatMetricConfiguration(loadMetricConfiguration())
    
    for row in voidreader:
        jsonRequest = []
        if not os.path.exists("/tmp/crawler/"):
            os.makedirs("/tmp/crawler/")
        g = rdflib.Graph()
        baseURI = row[1]
        try:
            result = g.parse(row[0])
        except rdflib.plugin.PluginException:
            result = g.parse(row[0], format=identifySerialisation(row[0]))
        for row in g.query("""SELECT ?dataset WHERE 
             { ?a <http://rdfs.org/ns/void#dataDump> ?dataset . }"""):           
             filename = "%s"%(row)
             print("downloading " + filename)
             urllib.urlretrieve(filename, "/tmp/crawler/"+ntpath.basename(filename))   
             if (ntpath.basename(filename).endswith(".tar.gz")):
                tar = tarfile.open("/tmp/crawler/"+ntpath.basename(filename))
                print("Extracting: "+ntpath.basename(filename))
                tar.extractall("/tmp/crawler/")
                tar.close()
                os.remove("/tmp/crawler/"+ntpath.basename(filename))
        datasetLocations = [ f for f in listdir("/tmp/crawler/") if isfile(join("/tmp/crawler/",f)) ]      
        datasetStr = "/tmp/crawler/"  
        datasetStr += ",/tmp/crawler/".join(datasetLocations);
        print(metricsConf)
        payload = {'Dataset' : datasetStr, 'QualityReportRequired' : 'false', 'MetricsConfiguration' : metricsConf, 'BaseUri' : baseURI }
        url="http://localhost:8080/Luzzu/compute_quality"
        r = requests.post(url, data=payload)
        r.text
        #os.remove("/tmp/crawler/")
    print('finished')
            
            