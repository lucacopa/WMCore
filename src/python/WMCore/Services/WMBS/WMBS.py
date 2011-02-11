import os
import pwd
import logging
from WMCore.Services.Service import Service
from WMCore.Wrappers import JsonWrapper

class WMBS(Service):

    """
    API for dealing with retrieving information from PhEDEx DataService
    """

    def __init__(self, dict={}):
        """
        responseType will be either xml or json
        """
        
        #if self.responseType == 'json':
            #self.parser = JSONParser()
        #elif self.responseType == 'xml':
            #self.parser = XMLParser()
        
        dict.setdefault("accept_type", "application/json")
        dict.setdefault("content_type", "application/json")
        self.encoder = JsonWrapper.dumps
        self.decoder = JsonWrapper.loads
        
        Service.__init__(self, dict)

    def _getResult(self, callname, clearCache = True,
                   args = None, verb="GET", contentType = None):
        """
        _getResult_

        retrieve JSON/XML formatted information given the service name and the
        argument dictionaries

        TODO: Probably want to move this up into Service
        """
        result = ''
        # make base file name from call name.
        file = callname.replace("/", "_")
        if clearCache:
            self.clearCache(file, args, verb)

        # can't pass the decoder here since refreshCache wright to file
        f = self.refreshCache(file, callname, args, encoder = self.encoder,
                              verb = verb, contentType = contentType)
        result = f.read()
        f.close()
        result = self.decoder(result)

        return result
    
    def getResourceInfo(self, tableFormat = True):
        """
        """
        callname = 'listthresholdsforcreate'
        args = {'tableFormat': tableFormat}
        return self._getResult(callname, args = args)

    def getSiteList(self):
        """
        """
        callname = 'listsites'
        return self._getResult(callname)
