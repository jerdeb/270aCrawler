import csv
import rdflib
import urllib
import ntpath
import os
import tarfile
import requests
import rdfextras
import urllib2
import logging
import sys

from os import listdir
from os.path import isfile, join
from rdflib.serializer import Serializer
from rdflib import plugin

# Logging configuration
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format="%(relativeCreated)d - %(name)s - %(levelname)s - %(message)s")
logger_crawl = logging.getLogger("crawler")
logger_crawl.setLevel(logging.DEBUG)

proxies = {
  # From uni-bonn servers use: "http": "http://webcache.iai.uni-bonn.de:3128"
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
    
def download(filename, folder):
    #urllib2.urlopen(filename, folder+ntpath.basename(filename))
    response = urllib2.urlopen(filename)
    fh = open(folder+ntpath.basename(filename), "w")
    fh.write(response.read())
    fh.close()
    

# Main
proxy = urllib2.ProxyHandler(proxies)
opener = urllib2.build_opener(proxy)
urllib2.install_opener(opener)

with open('voidlist.csv', 'rb') as csvfile:
    voidreader = csv.reader(csvfile, delimiter=",")
    metricsConf = formatMetricConfiguration(loadMetricConfiguration())
    
    for row in voidreader:
    	logger_crawl.debug("Reading resource: {0}, void: {1}".format(row[1], row[0]))
    	
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
             logger_crawl.info("Downloading resource: {0}. Into folder: {1}".format(filename, folder))
             download(filename, folder)
             
             if (ntpath.basename(filename).endswith(".tar.gz")):
             	logger_crawl.info("Extracting tar: {0} on directory: {1}".format(ntpath.basename(filename), folder))
                tar = tarfile.open(folder+ntpath.basename(filename))
                tar.extractall(folder)
                tar.close()
                os.remove(folder+ntpath.basename(filename))

        datasetLocations = [ "file:///"+join(folder,f) for f in listdir(folder) if isfile(join(folder,f)) ]      
        datasetStr = ",".join(datasetLocations);
        logger_crawl.info("Metrics config: {0}".format(metricsConf))
        
        payload = {'Dataset' : datasetStr, 'QualityReportRequired' : 'false', 'MetricsConfiguration' : metricsConf, 'BaseUri' : baseURI }
        url="http://localhost:8080/Luzzu/compute_quality"
        logger_crawl.debug("Sending POST. URL: {0}. Dataset: {1}. Base URI: {2}".format(url, datasetStr, baseURI))
        
        try:
            r = requests.post(url, data=payload, proxies=proxies)
            logger_crawl.info("Quality assessment completed for: {0}. Result: {1}", row[1], r.text)
        except Exception as ex:
            logger_crawl.exception("Error processing request. Crawling aborted.")
            break
        
    logger_crawl.info("Crawling finished")
    os.remove("/tmp/crawler/")    
