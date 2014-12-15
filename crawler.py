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

proxies = {
  "http": "http://webcache.iai.uni-bonn.de:3128",
}

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
        folder = "/tmp/crawler/"+row[1].replace('http://','').replace('/','')
        if not os.path.exists(folder):
            os.makedirs(folder)
        folder += '/'
        try:
            result = g.parse(row[0])
        except rdflib.plugin.PluginException:
            result = g.parse(row[0], format=identifySerialisation(row[0]))
        for row in g.query("""SELECT ?dataset WHERE 
             { ?a <http://rdfs.org/ns/void#dataDump> ?dataset . }"""):           
             filename = "%s"%(row)
             print("downloading " + filename)
             urllib.urlretrieve(filename, folder+ntpath.basename(filename))   
             if (ntpath.basename(filename).endswith(".tar.gz")):
                tar = tarfile.open(folder+ntpath.basename(filename))
                print("Extracting: "+ntpath.basename(filename))
                tar.extractall(folder)
                tar.close()
                os.remove(folder+ntpath.basename(filename))
        datasetLocations = [ f for f in listdir(folder) if isfile(join(folder,f)) ]      
        datasetStr = folder  
        datasetStr += ","+folder.join(datasetLocations);
        print(metricsConf)
        payload = {'Dataset' : datasetStr, 'QualityReportRequired' : 'false', 'MetricsConfiguration' : metricsConf, 'BaseUri' : baseURI }
        url="http://localhost:8080/Luzzu/compute_quality"
        r = requests.post(url, data=payload, proxies=proxies)
        r.text
    print('finished')
    os.remove("/tmp/crawler/")    
            