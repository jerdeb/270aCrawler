import csv
import rdflib
import urllib
import ntpath
import os
import tarfile
import requests

from os import listdir
from os.path import isfile, join

# Functions
def identifySerialisation( fileName ):
    #add all serialisations
    if fileName.endswith(".ttl"):
        return "turtle"
    else:
        return
        

# Main
with open('voidlist.csv', 'rb') as csvfile:
    voidreader = csv.reader(csvfile, delimiter=",")
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
        metricsConfiguration="[{\"@id\": \"urn:a\",\"@type\": [\"http://purl.org/eis/vocab/lmi#MetricConfiguration\"],\"http://purl.org/eis/vocab/lmi#metric\": [{\"@value\": \"eu.diachron.qualitymetrics.intrinsic.accuracy.POBODefinitionUsage\"}]}]"""
        payload = {'Dataset' : datasetStr, 'QualityReportRequired' : 'false', 'MetricsConfiguration' : metricsConfiguration, 'BaseUri' : baseURI }
        #url = "http://localhost:8080/Luzzu/compute_quality?Dataset="+datasetStr+"&QualityReportRequired=false&MetricsConfiguration="+metricsConfiguration+"&BaseUri='"+baseURI+"'"
        url="http://localhost:8080/Luzzu/compute_quality"
        r = requests.post(url, data=payload)
        #r = requests.post(url)
        r.text
        os.remove("/tmp/crawler/")
    print('finished')
            
            