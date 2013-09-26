"""
Perform cleanup actions
"""
__all__ = []



import threading
import logging
import time
import traceback
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.Services.WorkQueue.WorkQueue import WorkQueue as WorkQueueService
from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter
from WMComponent.AnalyticsDataCollector.DataCollectAPI import LocalCouchDBData, \
     WMAgentDBData, combineAnalyticsData, convertToRequestCouchDoc, \
     convertToAgentCouchDoc, initAgentInfo
from WMCore.WMFactory import WMFactory

class AgentStatusPoller(BaseWorkerThread):
    """
    Gether the summary data for request (workflow) from local queue,
    local job couchdb, wmbs/boss air and populate summary db for monitoring
    """
    def __init__(self, config):
        """
        initialize properties specified from config
        """
        BaseWorkerThread.__init__(self)
        # set the workqueue service for REST call
        self.config = config
        # need to get campaign, user, owner info
        self.agentInfo = initAgentInfo(self.config)
        self.summaryLevel = (config.AnalyticsDataCollector.summaryLevel).lower()
            
    def setup(self, parameters):
        """
        set db connection(couchdb, wmbs) to prepare to gather information
        """

        # interface to WMBS/BossAir db
        myThread = threading.currentThread()
        # set wmagent db data
        self.wmagentDB = WMAgentDBData(self.summaryLevel, myThread.dbi, myThread.logger)
        # set the connection for local couchDB call
        self.localSummaryCouchDB = WMStatsWriter(self.config.AnalyticsDataCollector.localWMStatsURL)
        self.centralWMStatsCouchDB = WMStatsWriter(self.config.AnalyticsDataCollector.centralWMStatsURL)

    def algorithm(self, parameters):
        """
        get information from wmbs, workqueue and local couch
        """
        try:
            logging.info("Getting Agent info ...")
            agentInfo = self.collectAgentInfo()
            
            #set the uploadTime - should be the same for all docs
            uploadTime = int(time.time())
            
            self.uploadAgentInfoToCentralWMStats(agentInfo, uploadTime)
            
            logging.info("Agent components down:\n %s" % agentInfo['down_components'])
            logging.info("Sleep for next WMStats alarm updating cycle")
            
        except Exception, ex:
            logging.error("Error occurred, will retry later:")
            logging.error(str(ex))
            logging.error("Trace back: \n%s" % traceback.format_exc())
   
    def checkLocalCouchServerStatus(self):
        localCouchServer = self.localSummaryCouchDB.getServerInstance()
        try:
            status = localCouchServer.status()
            replicationError = True
            for activeStatus in status['active_tasks']:
                if activeStatus["type"] == "Replication":
                    if self.config.AnalyticsDataCollector.centralWMStatsURL in activeStatus["task"]:
                        replicationError = False
                        break
            if replicationError:
                return {'status':'error', 'error_message': "replication stopped"}
            else:
                return {'status': 'ok'}
        except Exception, ex:
            return {'status':'down', 'error_message': str(ex)}

 
    def collectAgentInfo(self):
        #TODO: agent info (need to include job Slots for the sites)
        # always checks couch first
        source = self.config.JobStateMachine.jobSummaryDBName
        target = self.config.AnalyticsDataCollector.centralWMStatsURL
        couchInfo = self.checkLocalCouchServerStatus()
        logging.info("getting couchdb replication status: %s" % couchInfo)
        
        agentInfo = self.wmagentDB.getComponentStatus(self.config)
        agentInfo.update(self.agentInfo)
        
        if (couchInfo['status'] != 'ok'):
            agentInfo['down_components'].append("CouchServer")
            agentInfo['status'] = couchInfo['status']
            couchInfo['name'] = "CouchServer"
            agentInfo['down_component_detail'].append(couchInfo)
            
        return agentInfo

    def uploadAgentInfoToCentralWMStats(self, agentInfo, uploadTime):
        #direct data upload to the remote to prevent data conflict when agent is cleaned up and redeployed
        agentDocs = convertToAgentCouchDoc(agentInfo, self.config.ACDC, uploadTime)
        self.centralWMStatsCouchDB.updateAgentInfo(agentDocs)

