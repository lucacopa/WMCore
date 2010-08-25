#!/usr/bin/env python
#pylint: disable-msg=W0613
"""
The DBSUpload algorithm
"""
__all__ = []
__revision__ = "$Id: DBSUploadPoller.py,v 1.5 2009/09/02 16:16:10 mnorman Exp $"
__version__ = "$Revision: 1.5 $"
__author__ = "mnorman@fnal.gov"

import threading
import logging
import re
import os
import time
from sets import Set

import inspect

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from WMCore.WMFactory import WMFactory

from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Workflow import Workflow

from DBSAPI.dbsApi import DbsApi
#from DBSAPI.dbsException import *
#from DBSAPI.dbsApiException import *
#from DBSAPI.dbsAlgorithm import DbsAlgorithm

from WMComponent.DBSBuffer.Database.Interface.DBSBufferFile import DBSBufferFile

from WMCore.Services.DBS.DBSWriter import DBSWriter
from WMCore.Services.DBS           import DBSWriterObjects
from WMCore.Services.DBS.DBSErrors import DBSWriterError, formatEx,DBSReaderError
from WMCore.Services.DBS.DBSReader import DBSReader



class DBSUploadPoller(BaseWorkerThread):
    """
    Handles poll-based DBSUpload

    """


    def __init__(self, config, dbsconfig = None):
        """
        Initialise class members
        """
        myThread = threading.currentThread()
        myThread.dialect = os.getenv('DIALECT')
        BaseWorkerThread.__init__(self)
        self.config     = config
        self.dbsurl     = self.config.DBSUpload.dbsurl
        self.dbsversion = self.config.DBSUpload.dbsversion
        self.uploadFileMax = 10

        self.DBSMaxFiles   = self.config.DBSUpload.DBSMaxFiles
        self.DBSMaxSize    = self.config.DBSUpload.DBSMaxSize

        if dbsconfig == None:
            self.dbsconfig = config
    
    def setup(self, parameters):
        """
        Load DB objects required for queries
        """
        myThread = threading.currentThread()

        factory = WMFactory("dbsUpload", "WMComponent.DBSUpload.Database.Interface")
        self.dbinterface=factory.loadObject("UploadToDBS")

        bufferFactory = WMFactory("dbsBuffer", "WMComponent.DBSBuffer.Database.Interface")
        self.addToBuffer=bufferFactory.loadObject("AddToBuffer")

        logging.info("DBSURL %s"%self.dbsurl)
        args = { "url" : self.dbsurl, "level" : 'ERROR', "user" :'NORMAL', "version" : self.dbsversion }
        self.dbsapi = DbsApi(args)
        self.dbswriter = DBSWriter(self.dbsurl, level='ERROR', user='NORMAL', version=self.dbsversion)
        self.dbsreader = DBSReader(self.dbsurl, level='ERROR', user='NORMAL', version=self.dbsversion)





        return

    def uploadDatasets(self):
        """
        This should do the hard work of adding things to DBS.
        It essentially replaces BufferSuccess

        """

        #Initialize
        dbinterface = self.dbinterface
        addToBuffer = self.addToBuffer

        
        #Get datasets out of DBS
        datasets=dbinterface.findUploadableDatasets()

        myThread = threading.currentThread()

        #Now check them
        file_ids = []
        for dataset in datasets:
            #If we're here, then we have a dataset that needs to be uploaded.
            #First task, are the algos registered?
            algos = dbinterface.findAlgos(dataset)

            #Necessary for creating Process Datasets
            dataset['Conditions'] = None

            newAlgos = []

            for algo in algos:
                #if algo['InDBS'] == 0:
                #Then we have an algo that's not in DBS.  It needs to go there
                newAlgos.append(DBSWriterObjects.createAlgorithm(dict(algo), configMetadata = None, apiRef = self.dbsapi))
                addToBuffer.addAlgo(algo)

                    
            #Now all the algos should be there, so we can create the dataset
            #I'm unhappy about this because I don't know how to put a dataset in more then one algo
            primary = DBSWriterObjects.createPrimaryDataset(datasetInfo = dataset, apiRef = self.dbsapi)
            processed = DBSWriterObjects.createProcessedDataset(primaryDataset = primary, algorithm = newAlgos, \
                                                                datasetInfo = dataset, apiRef = self.dbsapi)
            addToBuffer.addDataset(dataset, 1)

            #Once the registration is done, you need to upload the individual files
            file_ids.extend(dbinterface.findUploadableFiles(dataset, self.uploadFileMax))
            
    	    files=[]
    	    #Making DBSBufferFile objects for easy manipulation
            for an_id in file_ids:
                file = DBSBufferFile(id=an_id['ID'])
                file.load(parentage=1)
                #Now really stupid stuff has to happen.
                initSet = file['locations']
                locations = Set()
                for loc in initSet:
                    locations.add(str(loc))
                file['locations'] = locations
                files.append(file)
                logging.info('I have prepared the file %s for uploading to DBS' %(an_id))

            #Now that you have the files, insert them as a list
            if len(files) > 0:
            	affectedBlocks = self.dbswriter.insertFilesForDBSBuffer(files = files, procDataset = dict(dataset), \
                                                       algos = algos, jobType = "NotMerge", insertDetectorData = False, \
                                                       maxFiles = self.DBSMaxFiles, maxSize = self.DBSMaxSize)
                for block in affectedBlocks:
                    info = block['StorageElementList']
                    locations = []
                    for loc in info:
                        locations.append(loc['Name'])
                    dbinterface.setBlockStatus(block['Name'], locations)

                #Update the file status, and then recount UnMigrated Files
            	dbinterface.updateFilesStatus(file_ids)
            

        return


    def terminate(self,params):
        logging.debug("terminating. doing one more pass before we die")
        self.algorithm(params)
        
    def algorithm(self, parameters):
        """
        Queries DB for all watched filesets, if matching filesets become
        available, create the subscriptions. Wraps in transaction.
        """
        logging.debug("Running subscription / fileset matching algorithm")
        myThread = threading.currentThread()
        try:
            myThread.transaction.begin()
            self.uploadDatasets()
            myThread.transaction.commit()
        except:
            myThread.transaction.rollback()
            raise



