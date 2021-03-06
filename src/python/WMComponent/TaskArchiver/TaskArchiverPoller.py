#!/usr/bin/env python
#pylint: disable=W0142
# W0142: Some people like ** magic
"""
The actual taskArchiver algorithm

Procedure:
a) Find and marks a finished all newly finished subscriptions
      This is defined by the Subscriptions.MarkNewFinishedSubscriptions DAO
b) Look for finished workflows as defined in the Workflow.GetFinishedWorkflows DAO
c) Upload couch summary information
d) Call WMBS.Subscription.deleteEverything() on all the associated subscriptions
e) Delete couch information and working directories

This should be a simple process.  Because of the long time between
the submission of subscriptions projected and the short time to run
this class, it should be run irregularly.


Config options
histogramKeys: Allows you to report values in histogram form in the
  workloadSummary - i.e., as a list of bins
histogramBins: Bin size for all histograms
histogramLimit: Limit in terms of number of standard deviations from the
  average at which you cut the histogram off.  All points outside of that
  go into overflow and underflow.
"""
__all__ = []

import re
import json
import logging
import os.path
import shutil
import tarfile
import httplib
import urllib2
import threading
import traceback

from WMCore.Algorithms                           import MathAlgos
from WMCore.DAOFactory                           import DAOFactory
from WMCore.Database.CMSCouch                    import CouchServer
from WMCore.Lexicon                              import sanitizeURL
from WMCore.Services.UserFileCache.UserFileCache import UserFileCache
from WMCore.WMBS.Subscription                    import Subscription
from WMCore.WMBS.Workflow                        import Workflow
from WMCore.WMException                          import WMException
from WMCore.WorkQueue.WorkQueue                  import localQueue
from WMCore.WorkQueue.WorkQueueExceptions        import WorkQueueNoMatchingElements
from WMCore.WorkerThreads.BaseWorkerThread       import BaseWorkerThread
from WMCore.BossAir.Plugins.gLitePlugin          import getDefaultDelegation
from WMCore.Credential.Proxy                     import Proxy
from WMComponent.JobCreator.CreateWorkArea       import getMasterName
from WMComponent.JobCreator.JobCreatorPoller     import retrieveWMSpec
from WMCore.Services.RequestManager.RequestManager import RequestManager
from WMCore.Services.ReqMgr.ReqMgr               import ReqMgr
from WMCore.Services.RequestDB.RequestDBWriter   import RequestDBWriter

from WMCore.DataStructs.MathStructs.DiscreteSummaryHistogram import DiscreteSummaryHistogram

class TaskArchiverPollerException(WMException):
    """
    _TaskArchiverPollerException_

    This is the class that serves as the customized
    Exception class for the TaskArchiverPoller

    As if you couldn't tell that already
    """



class FileEncoder(json.JSONEncoder):
    """
    JSON encoder to transform sets to lists and handle any object with a __to_json__ method
    """

    def default(self, obj):
        """
        Redefine JSONEncoder default method
        """
        if hasattr(obj, '__to_json__'):
            return obj.__to_json__()
        elif isinstance(obj, set):
            return list(obj)
        return json.JSONEncoder.default(self, obj)

def getProxy(config, userdn, group, role):
    """
    _getProxy_
    """
    defaultDelegation = getDefaultDelegation(config, "cms", "myproxy.cern.ch", threading.currentThread().logger)
    defaultDelegation['userDN'] = userdn
    defaultDelegation['group'] = group
    defaultDelegation['role'] = role

    logging.debug("Retrieving proxy for %s" % userdn)
    proxy = Proxy(defaultDelegation)
    proxyPath = proxy.getProxyFilename(True)
    timeleft = proxy.getTimeLeft(proxyPath)
    if timeleft is not None and timeleft > 3600:
        return (True, proxyPath)
    proxyPath = proxy.logonRenewMyProxy()
    timeleft = proxy.getTimeLeft(proxyPath)
    if timeleft is not None and timeleft > 0:
        return (True, proxyPath)
    return (False, None)

def uploadPublishWorkflow(config, workflow, ufcEndpoint, workDir):
    """
    Write out and upload to the UFC a JSON file
    with all the info needed to publish this dataset later
    """
    retok, proxyfile = getProxy(config, workflow.dn, workflow.vogroup, workflow.vorole)
    if not retok:
        logging.info("Cannot get the user's proxy")
        return False

    ufc = UserFileCache({'endpoint': ufcEndpoint, 'cert': proxyfile, 'key': proxyfile})

    # Skip tasks ending in LogCollect, they have nothing interesting.
    taskNameParts = workflow.task.split('/')
    if taskNameParts.pop() in ['LogCollect']:
        logging.info('Skipping LogCollect task')
        return False
    logging.info('Generating JSON for publication of %s of type %s' % (workflow.name, workflow.wfType))

    myThread = threading.currentThread()

    dbsDaoFactory = DAOFactory(package = "WMComponent.DBS3Buffer",
                                     logger = myThread.logger, 
                                     dbinterface = myThread.dbi)
    
    findFiles = dbsDaoFactory(classname = "LoadFilesByWorkflow")

    # Fetch and filter the files to the ones we actually need
    uploadDatasets = {}
    uploadFiles = findFiles.execute(workflowName = workflow.name)
    for file in uploadFiles:
        datasetName = file['datasetPath']
        if datasetName not in uploadDatasets:
            uploadDatasets[datasetName] = []
        uploadDatasets[datasetName].append(file)

    if not uploadDatasets:
        logging.info('No datasets found to upload.')
        return False

    # Write JSON file and then create tarball with it
    baseName = '%s_publish.tgz'  % workflow.name
    jsonName = os.path.join(workDir, '%s_publish.json' % workflow.name)
    tgzName = os.path.join(workDir, baseName)
    with open(jsonName, 'w') as jsonFile:
        json.dump(uploadDatasets, fp=jsonFile, cls=FileEncoder, indent=2)

    # Only in 2.7 does tarfile become usable as context manager
    tgzFile = tarfile.open(name=tgzName, mode='w:gz')
    tgzFile.add(jsonName)
    tgzFile.close()

    result = ufc.upload(fileName=tgzName, name=baseName)
    logging.debug('Upload result %s' % result)
    # If this doesn't work, exception will propogate up and block archiving the task
    logging.info('Uploaded with name %s and hashkey %s' % (result['name'], result['hashkey']))
    return




class TaskArchiverPoller(BaseWorkerThread):
    """
    Polls for Ended jobs

    List of attributes

    requireCouch:  raise an exception on couch failure instead of ignoring
    """
    def __init__(self, config):
        """
        Initialise class members
        """
        BaseWorkerThread.__init__(self)

        myThread = threading.currentThread()
        self.daoFactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)
        
        self.dbsDaoFactory = DAOFactory(package = "WMComponent.DBS3Buffer",
                                     logger = myThread.logger, 
                                     dbinterface = myThread.dbi)

        self.config      = config
        self.jobCacheDir = self.config.JobCreator.jobCacheDir

        if getattr(self.config.TaskArchiver, "useWorkQueue", False) != False:
            # Get workqueue setup from config unless overridden
            if hasattr(self.config.TaskArchiver, 'WorkQueueParams'):
                self.workQueue = localQueue(**self.config.TaskArchiver.WorkQueueParams)
            else:
                from WMCore.WorkQueue.WorkQueueUtils import queueFromConfig
                self.workQueue = queueFromConfig(self.config)
        else:
            self.workQueue = None

        self.maxProcessSize    = getattr(self.config.TaskArchiver, 'maxProcessSize', 250)
        self.timeout           = getattr(self.config.TaskArchiver, "timeOut", None)
        self.nOffenders        = getattr(self.config.TaskArchiver, 'nOffenders', 3)
        self.useReqMgrForCompletionCheck   = getattr(self.config.TaskArchiver, 'useReqMgrForCompletionCheck', True)
        self.uploadPublishInfo = getattr(self.config.TaskArchiver, 'uploadPublishInfo', False)
        self.uploadPublishDir  = getattr(self.config.TaskArchiver, 'uploadPublishDir', None)
        self.userFileCacheURL  = getattr(self.config.TaskArchiver, 'userFileCacheURL', None)

        # Set up optional histograms
        self.histogramKeys  = getattr(self.config.TaskArchiver, "histogramKeys", [])
        self.histogramBins  = getattr(self.config.TaskArchiver, "histogramBins", 10)
        self.histogramLimit = getattr(self.config.TaskArchiver, "histogramLimit", 5.0)
        
        # Set defaults for reco performance reporting
        self.interestingPDs = getattr(config.TaskArchiver, "perfPrimaryDatasets", ['SingleMu', 'MuHad'])
        self.dqmUrl         = getattr(config.TaskArchiver, "dqmUrl", 'https://cmsweb.cern.ch/dqm/dev/')        
        self.perfDashBoardMinLumi = getattr(config.TaskArchiver, "perfDashBoardMinLumi", 50)
        self.perfDashBoardMaxLumi = getattr(config.TaskArchiver, "perfDashBoardMaxLumi", 9000)
        self.dashBoardUrl = getattr(config.TaskArchiver, "dashBoardUrl", None)
        
        if not self.useReqMgrForCompletionCheck:
            #sets the local monitor summary couch db
            self.requestLocalCouchDB = RequestDBWriter(self.config.AnalyticsDataCollector.localT0RequestDBURL, 
                                                   couchapp = self.config.AnalyticsDataCollector.RequestCouchApp)
            self.centralCouchDBWriter = self.requestLocalCouchDB;
        else:
            self.centralCouchDBWriter = RequestDBWriter(self.config.AnalyticsDataCollector.centralRequestDBURL)
            
            if self.config.TaskArchiver.reqmgr2Only:
                self.reqmgr2Svc = ReqMgr(self.config.TaskArchiver.ReqMgr2ServiceURL)
            else:
                #TODO this need to be remove when reqmgr is not used
                self.reqmgrSvc = RequestManager({'endpoint': self.config.TaskArchiver.ReqMgrServiceURL})
            
        # Start a couch server for getting job info
        # from the FWJRs for committal to archive
        try:
            workDBName       = getattr(self.config.TaskArchiver, 'workloadSummaryCouchDBName',
                                        'workloadsummary')
            workDBurl        = getattr(self.config.TaskArchiver, 'workloadSummaryCouchURL')
            jobDBurl         = sanitizeURL(self.config.JobStateMachine.couchurl)['url']
            jobDBName        = self.config.JobStateMachine.couchDBName
            self.jobCouchdb  = CouchServer(jobDBurl)
            self.workCouchdb = CouchServer(workDBurl)

            self.jobsdatabase = self.jobCouchdb.connectDatabase("%s/jobs" % jobDBName)
            self.fwjrdatabase = self.jobCouchdb.connectDatabase("%s/fwjrs" % jobDBName)
            self.workdatabase = self.workCouchdb.connectDatabase(workDBName)

            logging.debug("Using url %s/%s for job" % (jobDBurl, jobDBName))
            logging.debug("Writing to  %s/%s for workloadSummary" % (sanitizeURL(workDBurl)['url'], workDBName))
            self.requireCouch = getattr(self.config.TaskArchiver, 'requireCouch', False)
        except Exception as ex:
            msg =  "Error in connecting to couch.\n"
            msg += str(ex)
            logging.error(msg)
            self.jobsdatabase = None
            self.fwjrdatabase = None
            if getattr(self.config.TaskArchiver, 'requireCouch', False):
                raise TaskArchiverPollerException(msg)

        # initialize the alert framework (if available)
        self.initAlerts(compName = "TaskArchiver")

        #Load the cleanout state ID and save it
        stateIDDAO = self.daoFactory(classname = "Jobs.GetStateID")
        self.stateID = stateIDDAO.execute("cleanout")

        return

    def terminate(self, params):
        """
        _terminate_

        This function terminates the job after a final pass
        """
        logging.debug("terminating. doing one more pass before we die")
        self.algorithm(params)
        return

    def algorithm(self, parameters = None):
        """
        _algorithm_

        Executes the two main methods of the poller:
        1. findAndMarkFinishedSubscriptions
        2. archiveTasks
        Final result is that finished workflows get their summary built and uploaded to couch,
        and all traces of them are removed from the agent WMBS and couch (this last one on demand).
        """
        try:
            self.findAndMarkFinishedSubscriptions()
            self.archiveTasks()
            self.deleteWorkflowFromWMBSAndDisk()
        except WMException:
            myThread = threading.currentThread()
            if getattr(myThread, 'transaction', False) \
                   and getattr(myThread.transaction, 'transaction', False):
                myThread.transaction.rollback()
            raise
        except Exception as ex:
            myThread = threading.currentThread()
            msg = "Caught exception in TaskArchiver\n"
            msg += str(ex)
            if getattr(myThread, 'transaction', False) \
                   and getattr(myThread.transaction, 'transaction', False):
                myThread.transaction.rollback()
            raise TaskArchiverPollerException(msg)

        return

    def findAndMarkFinishedSubscriptions(self):
        """
        _findAndMarkFinishedSubscriptions_

        Find new finished subscriptions and mark as finished in WMBS.
        """
        myThread = threading.currentThread()

        myThread.transaction.begin()

        #Get the subscriptions that are now finished and mark them as such
        logging.info("Polling for finished subscriptions")
        finishedSubscriptions = self.daoFactory(classname = "Subscriptions.MarkNewFinishedSubscriptions")
        finishedSubscriptions.execute(self.stateID, timeOut = self.timeout)
        logging.info("Finished subscriptions updated")

        myThread.transaction.commit()

        return

    def archiveTasks(self):
        """
        _archiveTasks_

        This method will call several auxiliary methods to do the following:
        1. Get finished workflows (a finished workflow is defined in Workflow.GetFinishedWorkflows)
        2. Gather the summary information from each workflow/task and upload it to couch
        3. Publish to DashBoard Reconstruction performance information
        4. Notify the WorkQueue about finished subscriptions
        5. If all succeeds, delete all information about the workflow from couch and WMBS
        """
        #Get the finished workflows, in descending order
        finishedWorkflowsDAO = self.daoFactory(classname = "Workflow.GetFinishedWorkflows")
        finishedwfs = finishedWorkflowsDAO.execute()

        #Only delete those where the upload and notification succeeded
        logging.info("Found %d candidate workflows for completing: %s" % (len(finishedwfs),finishedwfs.keys()))
        # update the completed flag in dbsbuffer_workflow table so blocks can be closed
        # create updateDBSBufferWorkflowComplete DAO
        if len(finishedwfs) == 0:
            return
        
        completedWorkflowsDAO = self.dbsDaoFactory(classname = "UpdateWorkflowsToCompleted")
        
        centralCouchAlive = True
        try:
            #TODO: need to enable when reqmgr2 -wmstats is ready
            #abortedWorkflows = self.reqmgrCouchDBWriter.getRequestByStatus(["aborted"], format = "dict");
            abortedWorkflows = self.centralCouchDBWriter.getRequestByStatus(["aborted"])
            logging.info("There are %d requests in 'aborted' status in central couch." % len(abortedWorkflows))
            forceCompleteWorkflows = self.centralCouchDBWriter.getRequestByStatus(["force-complete"])
            logging.info("List of 'force-complete' workflows in central couch: %s" % forceCompleteWorkflows)
            
        except Exception as ex:
            centralCouchAlive = False
            logging.error("we will try again when remote couch server comes back\n%s" % str(ex))
        
        if centralCouchAlive:
            for workflow in finishedwfs:
                try:
                    #Upload summary to couch
                    spec = retrieveWMSpec(wmWorkloadURL = finishedwfs[workflow]["spec"])
                    if not spec:
                        raise Exception(msg = "Couldn't load spec for %s" % workflow)
                    self.archiveWorkflowSummary(spec = spec)
                    
                    #Send Reconstruciton performance information to DashBoard
                    if self.dashBoardUrl != None:
                        self.publishRecoPerfToDashBoard(spec)

                    #Notify the WorkQueue, if there is one
                    if self.workQueue != None:
                        subList = []
                        logging.info("Marking subscriptions as Done ...")
                        for l in finishedwfs[workflow]["workflows"].values():
                            subList.extend(l)
                        self.notifyWorkQueue(subList)
                    
                    #Now we know the workflow as a whole is gone, we can delete the information from couch
                    if not self.useReqMgrForCompletionCheck:
                        self.requestLocalCouchDB.updateRequestStatus(workflow, "completed")
                        logging.info("status updated to completed %s" % workflow)
    
                    if workflow in abortedWorkflows:
                        #TODO: remove when reqmgr2-wmstats deployed
                        newState = "aborted-completed"
                    elif workflow in forceCompleteWorkflows:
                        newState = "completed"
                    else:
                        newState = None
                        
                    if newState != None:
                        # update reqmgr workload document only request mgr is installed
                        if not self.useReqMgrForCompletionCheck:
                            # commented out untill all the agent is updated so every request have new state
                            # TODO: agent should be able to write reqmgr db diretly add the right group in
                            # reqmgr
                            self.requestLocalCouchDB.updateRequestStatus(workflow, newState)
                        else:
                            if self.config.TaskArchiver.reqmgr2Only:
                                self.reqmgr2Svc.updateRequestStatus(workflow, newState)
                            else:
                                #TODO this need to be remove when reqmgr is not used 
                                logging.info("Updating status to '%s' in both oracle and couchdb ..." % newState)
                                self.reqmgrSvc.updateRequestStatus(workflow, newState)
                            
                        logging.info("status updated to '%s' : %s" % (newState, workflow))
                    
                    completedWorkflowsDAO.execute([workflow])
        
                except TaskArchiverPollerException as ex:

                    #Something didn't go well when notifying the workqueue, abort!!!
                    logging.error("Something bad happened while archiving tasks.")
                    logging.error(str(ex))
                    self.sendAlert(1, msg = str(ex))
                    continue
                except Exception as ex:
                    #Something didn't go well on couch, abort!!!
                    msg = "Couldn't upload summary for workflow %s, will try again next time\n" % workflow
                    msg += "Nothing will be deleted until the summary is in couch\n"
                    msg += "Exception message: %s" % str(ex)
                    msg += "\nTraceback: %s" % traceback.format_exc()
                    logging.error(msg)
                    self.sendAlert(3, msg = msg)
                    continue
        return

    def notifyWorkQueue(self, subList):
        """
        _notifyWorkQueue_

        Tells the workQueue component that a particular subscription,
        or set of subscriptions, is done.  Receives confirmation
        """

        for sub in subList:
            try:
                self.workQueue.doneWork(SubscriptionId = sub)
            except WorkQueueNoMatchingElements:
                #Subscription wasn't known to WorkQueue, feel free to clean up
                logging.info("Local WorkQueue knows nothing about this subscription: %s" % sub)
                pass
            except Exception as ex:
                msg = "Error talking to workqueue: %s\n" % str(ex)
                msg += "Tried to complete the following: %s\n" % sub
                raise TaskArchiverPollerException(msg)

        return

    
    def deleteWorkflowFromWMBSAndDisk(self):
        #Get the finished workflows, in descending order
        deletableWorkflowsDAO = self.daoFactory(classname = "Workflow.GetDeletableWorkflows")
        deletablewfs = deletableWorkflowsDAO.execute()

        #Only delete those where the upload and notification succeeded
        logging.info("Found %d candidate workflows for deletion: %s" % (len(deletablewfs), deletablewfs.keys()))
        # update the completed flag in dbsbuffer_workflow table so blocks can be closed
        # create updateDBSBufferWorkflowComplete DAO
        if len(deletablewfs) == 0:
            return
        safeStatesToDelete = ["completed", "aborted-completed", "rejected", "announced",
                              "normal-archived", "aborted-archived", "rejected-archived"]
        wfsToDelete = {}
        for workflow in deletablewfs:
            try:
                spec = retrieveWMSpec(wmWorkloadURL = deletablewfs[workflow]["spec"])
                if not spec:
                    raise Exception(msg = "Couldn't load spec for %s" % workflow)
                # check workfow is alredy completed or archived (which means
                
                #This is used both tier0 and normal agent case
                result = self.centralCouchDBWriter.getStatusAndTypeByRequest(workflow)
                wfStatus = result[workflow][0]
                if wfStatus in safeStatesToDelete:
                    wfsToDelete[workflow] = {"spec" : spec, "workflows": deletablewfs[workflow]["workflows"]}
                else:
                    logging.error("%s is in %s, will be deleted later" % (workflow, wfStatus))
            
            except Exception, ex:
                #Something didn't go well on couch, abort!!!
                msg = "Couldn't delete %s " % workflow
                msg += "Exception message: %s" % str(ex)
                msg += "\nTraceback: %s" % traceback.format_exc()
                logging.error(msg)
                continue

        logging.info("Time to kill %d workflows." % len(wfsToDelete))
        self.killWorkflows(wfsToDelete)
        
    def killWorkflows(self, workflows):
        """
        _killWorkflows_

        Delete all the information in couch and WMBS about the given
        workflow, go through all subscriptions and delete one by
        one.
        The input is a dictionary with workflow names as keys, fully loaded WMWorkloads and
        subscriptions lists as values
        """
        for workflow in workflows:
            logging.info("Deleting workflow %s" % workflow)
            try:
                #Get the task-workflow ids, sort them by ID,
                #higher ID first so we kill
                #the leaves of the tree first, root last
                workflowsIDs = workflows[workflow]["workflows"].keys()
                workflowsIDs.sort(reverse = True)

                #Now go through all tasks and load the WMBS workflow objects
                wmbsWorkflows = []
                for wfID in workflowsIDs:
                    wmbsWorkflow = Workflow(id = wfID)
                    wmbsWorkflow.load()
                    wmbsWorkflows.append(wmbsWorkflow)

                #Time to shoot one by one
                for wmbsWorkflow in wmbsWorkflows:
                    if self.uploadPublishInfo:
                        self.createAndUploadPublish(wmbsWorkflow)

                    #Load all the associated subscriptions and shoot them one by one
                    subIDs = workflows[workflow]["workflows"][wmbsWorkflow.id]
                    for subID in subIDs:
                        subscription = Subscription(id = subID)
                        subscription['workflow'] = wmbsWorkflow
                        subscription.load()
                        subscription.deleteEverything()

                    #Check that the workflow is gone
                    if wmbsWorkflow.exists():
                        #Something went bad, this workflow
                        #should be gone by now
                        msg = "Workflow %s, Task %s was not deleted completely" % (wmbsWorkflow.name,
                                                                                   wmbsWorkflow.task)
                        raise TaskArchiverPollerException(msg)

                    #Now delete directories
                    _, taskDir = getMasterName(startDir = self.jobCacheDir,
                                               workflow = wmbsWorkflow)
                    logging.info("About to delete work directory %s" % taskDir)
                    if os.path.exists(taskDir):
                        if os.path.isdir(taskDir):
                            shutil.rmtree(taskDir)
                        else:
                            # What we think of as a working directory is not a directory
                            # This should never happen and there is no way we can recover
                            # from this here. Bail out now and have someone look at things.
                            msg = "Work directory is not a directory, this should never happen: %s" % taskDir
                            raise TaskArchiverPollerException(msg)
                    else:
                        msg = "Attempted to delete work directory but it was already gone: %s" % taskDir
                        logging.debug(msg)

                spec = workflows[workflow]["spec"]
                topTask = spec.getTopLevelTask()[0]

                # Now take care of the sandbox
                sandbox = getattr(topTask.data.input, 'sandbox', None)
                if sandbox:
                    sandboxDir = os.path.dirname(sandbox)
                    if os.path.isdir(sandboxDir):
                        shutil.rmtree(sandboxDir)
                        logging.debug("Sandbox dir deleted")
                    else:
                        logging.error("Attempted to delete sandbox dir but it was already gone: %s" % sandboxDir)

            except Exception as ex:
                msg = "Critical error while deleting workflow %s\n" % workflow
                msg += str(ex)
                msg += str(traceback.format_exc())
                logging.error(msg)
                self.sendAlert(2, msg = msg)

    def archiveWorkflowSummary(self, spec):
        """
        _archiveWorkflowSummary_

        For each workflow pull its information from couch and WMBS and turn it into
        a summary for archiving
        """

        failedJobs = []

        workflowData = {'retryData': {}}
        workflowName = spec.name()

        #First make sure that we didn't upload something already
        #Could be the that the WMBS deletion epic failed,
        #so we can skip this if there is a summary already up there
        #TODO: With multiple agents sharing workflows, we will need to differentiate and combine summaries for a request
        if self.workdatabase.documentExists(workflowName):
            logging.info("Couch summary for %s already exists, proceeding only with cleanup" % workflowName)
            return

        # Set campaign
        workflowData['campaign'] = spec.getCampaign()
        # Set inputdataset
        workflowData['inputdatasets'] = spec.listInputDatasets()
        # Set histograms
        histograms = {'workflowLevel' : {'failuresBySite' :
                                         DiscreteSummaryHistogram('Failed jobs by site', 'Site')},
                      'taskLevel' : {},
                      'stepLevel' : {}}

        # Get a list of failed job IDs
        # Make sure you get it for ALL tasks in the spec
        for taskName in spec.listAllTaskPathNames():
            failedTmp = self.jobsdatabase.loadView("JobDump", "failedJobsByWorkflowName",
                                                   options = {"startkey": [workflowName, taskName],
                                                              "endkey": [workflowName, taskName],
                                                              "stale" : "update_after"})['rows']
            for entry in failedTmp:
                failedJobs.append(entry['value'])

        retryData = self.jobsdatabase.loadView("JobDump", "retriesByTask",
                                               options = {'group_level': 3,
                                                          'startkey': [workflowName],
                                                          'endkey': [workflowName, {}],
                                                          "stale" : "update_after"})['rows']
        for row in retryData:
            taskName = row['key'][2]
            count = str(row['key'][1])
            if not taskName in workflowData['retryData'].keys():
                workflowData['retryData'][taskName] = {}
            workflowData['retryData'][taskName][count] = row['value']

        output = self.fwjrdatabase.loadView("FWJRDump", "outputByWorkflowName",
                                            options = {"group_level": 2,
                                                       "startkey": [workflowName],
                                                       "endkey": [workflowName, {}],
                                                       "group": True,
                                                       "stale" : "update_after"})['rows']
        outputList = {}
        try:
            outputListStr = self.fwjrdatabase.loadList("FWJRDump", "workflowOutputTaskMapping",
                                                    "outputByWorkflowName", options = {"startkey": [workflowName],
                                                                                       "endkey": [workflowName, {}],
                                                                                       "reduce": False})
            outputList = json.loads(outputListStr)
        except Exception as ex:
            # Catch couch errors
            logging.error("Could not load the output task mapping list due to an error")
            logging.error("Error: %s" % str(ex))
        perf = self.handleCouchPerformance(workflowName = workflowName)
        workflowData['performance'] = {}
        for key in perf:
            workflowData['performance'][key] = {}
            for attr in perf[key].keys():
                workflowData['performance'][key][attr] = perf[key][attr]


        workflowData["_id"] = workflowName
        try:
            workflowData["ACDCServer"] = sanitizeURL(self.config.ACDC.couchurl)['url']
            workflowData["ACDCDatabase"] = self.config.ACDC.database
        except AttributeError as ex:
            # We're missing the ACDC info.
            # Keep going
            logging.error("ACDC info missing from config.  Skipping this step in the workflow summary.")
            logging.error("Error: %s" % str(ex))


        # Attach output
        workflowData['output'] = {}
        for e in output:
            entry = e['value']
            dataset = entry['dataset']
            workflowData['output'][dataset] = {}
            workflowData['output'][dataset]['nFiles'] = entry['count']
            workflowData['output'][dataset]['size']   = entry['size']
            workflowData['output'][dataset]['events'] = entry['events']
            workflowData['output'][dataset]['tasks']  = outputList.get(dataset, {}).keys()

        # If the workflow was aborted, then don't parse all the jobs, cut at 5k
        try:
            reqDetails = self.centralCouchDBWriter.getRequestByNames(workflowName)
            wfStatus = reqDetails[workflowName]['RequestTransition'][-1]['Status']
            if wfStatus in ["aborted", "aborted-completed", "aborted-archived"]:
                logging.info("Workflow %s in status %s with a total of %d jobs, capping at 5000" % (
                    workflowName, wfStatus, len(failedJobs)))
                failedJobs = failedJobs[:5000]
        except Exception as ex:
            logging.error("Failed to query getRequestByNames view. Will retry later.\n%s" % str(ex))

        # Loop over all failed jobs
        workflowData['errors'] = {}

        #Get the job information from WMBS, a la ErrorHandler
        #This will probably take some time, better warn first
        logging.info("Starting to load  the failed job information")
        logging.info("This may take some time")

        #Let's split the list of failed jobs in chunks
        while len(failedJobs) > 0:
            chunkList = failedJobs[:self.maxProcessSize]
            failedJobs = failedJobs[self.maxProcessSize:]
            logging.info("Processing %d this cycle, %d jobs remaining" % (self.maxProcessSize, len(failedJobs)))

            loadJobs = self.daoFactory(classname = "Jobs.LoadForTaskArchiver")
            jobList = loadJobs.execute(chunkList)
            logging.info("Processing %d jobs," % len(jobList))
            for job in jobList:
                lastRegisteredRetry = None
                errorCouch = self.fwjrdatabase.loadView("FWJRDump", "errorsByJobID",
                                                        options = {"startkey": [job['id'], 0],
                                                                   "endkey": [job['id'], {}],
                                                                   "stale" : "update_after"})['rows']

                #Get the input files
                inputLFNs = [x['lfn'] for x in job['input_files']]
                runs = []
                for inputFile in job['input_files']:
                    runs.extend(inputFile.getRuns())

                # Get rid of runs that aren't in the mask
                mask = job['mask']
                runs = mask.filterRunLumisByMask(runs = runs)
                
                logging.info("Processing %d errors, for job id %s" % (len(errorCouch), job['id']))
                for err in errorCouch:
                    task   = err['value']['task']
                    step   = err['value']['step']
                    errors = err['value']['error']
                    logs   = err['value']['logs']
                    start  = err['value']['start']
                    stop   = err['value']['stop']
                    errorSite = str(err['value']['site'])
                    retry = err['value']['retry']
                    if lastRegisteredRetry is None or lastRegisteredRetry != retry:
                        histograms['workflowLevel']['failuresBySite'].addPoint(errorSite, 'Failed Jobs')
                        lastRegisteredRetry = retry
                    if task not in histograms['stepLevel']:
                        histograms['stepLevel'][task] = {}
                    if step not in histograms['stepLevel'][task]:
                        histograms['stepLevel'][task][step] = {'errorsBySite' : DiscreteSummaryHistogram('Errors by site',
                                                                                                         'Site')}
                    errorsBySiteData = histograms['stepLevel'][task][step]['errorsBySite']
                    if not task in workflowData['errors'].keys():
                        workflowData['errors'][task] = {'failureTime': 0}
                    if not step in workflowData['errors'][task].keys():
                        workflowData['errors'][task][step] = {}
                    workflowData['errors'][task]['failureTime'] += (stop - start)
                    stepFailures = workflowData['errors'][task][step]
                    for error in errors:
                        exitCode = str(error['exitCode'])
                        if not exitCode in stepFailures.keys():
                            stepFailures[exitCode] = {"errors": [],
                                                      "jobs":   0,
                                                      "input":  [],
                                                      "runs":   {},
                                                      "logs":   []}
                        stepFailures[exitCode]['jobs'] += 1 # Increment job counter
                        errorsBySiteData.addPoint(errorSite, str(exitCode))
                        if len(stepFailures[exitCode]['errors']) == 0 or \
                               exitCode == '99999':
                            # Only record the first error for an exit code
                            # unless exit code is 99999 (general panic)
                            stepFailures[exitCode]['errors'].append(error)
                        # Add input LFNs to structure
                        for inputLFN in inputLFNs:
                            if not inputLFN in stepFailures[exitCode]['input']:
                                stepFailures[exitCode]['input'].append(inputLFN)
                        # Add runs to structure
                        for run in runs:
                            if not str(run.run) in stepFailures[exitCode]['runs'].keys():
                                stepFailures[exitCode]['runs'][str(run.run)] = []
                            logging.debug("number of lumis failed: %s" % len(run.lumis))    
                            if len(run.lumis) > 10000:
                                logging.warning("too many lumis: %s in job id: %s inputfiles: %s" % (
                                                len(run.lumis), job['id'], inputLFNs))
                                continue
                            nodupLumis = set(run.lumis)    
                            for l in nodupLumis:
                                stepFailures[exitCode]['runs'][str(run.run)].append(l)
                        for log in logs:
                            if not log in stepFailures[exitCode]["logs"]:
                                stepFailures[exitCode]["logs"].append(log)
        # Adding logArchives per task
        logArchives = self.getLogArchives(spec)
        workflowData['logArchives'] = logArchives

        jsonHistograms = {'workflowLevel' : {},
                          'taskLevel' : {},
                          'stepLevel' : {}}
        for histogram in histograms['workflowLevel']:
            jsonHistograms['workflowLevel'][histogram] = histograms['workflowLevel'][histogram].toJSON()
        for task in histograms['taskLevel']:
            jsonHistograms['taskLevel'][task] = {}
            for histogram in histograms['taskLevel'][task]:
                jsonHistograms['taskLevel'][task][histogram] = histograms['taskLevel'][task][histogram].toJSON()
        for task in histograms['stepLevel']:
            jsonHistograms['stepLevel'][task] = {}
            for step in histograms['stepLevel'][task]:
                jsonHistograms['stepLevel'][task][step] = {}
                for histogram in histograms['stepLevel'][task][step]:
                    jsonHistograms['stepLevel'][task][step][histogram] = histograms['stepLevel'][task][step][histogram].toJSON()

        workflowData['histograms'] = jsonHistograms

        # Now we have the workflowData in the right format
        # Time to send them on
        logging.info("About to commit workflow summary for %s" % workflowName)
        self.workdatabase.commitOne(workflowData)

        logging.info("Finished committing workflow summary to couch")

        return
    def getLogArchives(self, spec):
        """
        _getLogArchives_

        Gets per Workflow/Task what are the log archives, sends it to the summary to be displayed on the page
        """
        try:
            logArchivesTaskStr = self.fwjrdatabase.loadList("FWJRDump", "logCollectsByTask",
                                                            "logArchivePerWorkflowTask", options = {"reduce" : False},
                                                            keys = spec.listAllTaskPathNames())
            logArchivesTask = json.loads(logArchivesTaskStr)
            return logArchivesTask
        except Exception as ex:
            logging.error("Couldn't load the logCollect list from CouchDB.")
            logging.error("Error: %s" % str(ex))
            return {}

    def handleCouchPerformance(self, workflowName):
        """
        _handleCouchPerformance_

        The couch performance stuff is convoluted enough I think I want to handle it separately.
        """
        perf = self.fwjrdatabase.loadView("FWJRDump", "performanceByWorkflowName",
                                          options = {"startkey": [workflowName],
                                                     "endkey": [workflowName],
                                                     "stale" : "update_after"})['rows']
                                                     
        failedJobs = self.getFailedJobs(workflowName)

        taskList  = {}
        finalTask = {}

        for row in perf:
            taskName = row['value']['taskName']
            stepName = row['value']['stepName']
            if not taskName in taskList.keys():
                taskList[taskName] = {}
            if not stepName in taskList[taskName].keys():
                taskList[taskName][stepName] = []
            value = row['value']
            taskList[taskName][stepName].append(value)

        for taskName in taskList.keys():
            final = {}
            for stepName in taskList[taskName].keys():
                output = {'jobTime': []}
                outputFailed = {'jobTime': []} # This will be same, but only for failed jobs
                final[stepName] = {}
                masterList = []

                # For each step put the data into a dictionary called output
                # keyed by the name of the value
                for row in taskList[taskName][stepName]:
                    masterList.append(row)
                    for key in row.keys():
                        if key in ['startTime', 'stopTime', 'taskName', 'stepName', 'jobID']:
                            continue
                        if not key in output.keys():
                            output[key] = []
                            if len(failedJobs) > 0 :
                                outputFailed[key] = []
                        try:
                            output[key].append(float(row[key]))
                            if row['jobID'] in failedJobs:
                                outputFailed[key].append(float(row[key]))
                                
                        except TypeError:
                            # Why do we get None values here?
                            # We may want to look into it
                            logging.debug("Got a None performance value for key %s" % key)
                            if row[key] == None:
                                output[key].append(0.0)
                            else:
                                raise
                    try:
                        jobTime = row.get('stopTime', None) - row.get('startTime', None)
                        output['jobTime'].append(jobTime)
                        row['jobTime'] = jobTime
                        # Account job running time here only if the job has failed
                        if (row['jobID'] in failedJobs):
                            outputFailed['jobTime'].append(jobTime)
                    except TypeError:
                        # One of those didn't have a real value
                        pass

                # Now that we've sorted the data, we process it one key at a time
                for key in output.keys():
                    final[stepName][key] = {}
                    # Assemble the 'worstOffenders'
                    # These are the top [self.nOffenders] in that particular category
                    # i.e., those with the highest values
                    offenders = MathAlgos.getLargestValues(dictList = masterList, key = key,
                                                           n = self.nOffenders)
                    for x in offenders:
                        try:
                            logArchive = self.fwjrdatabase.loadView("FWJRDump", "logArchivesByJobID",
                                                                    options = {"startkey": [x['jobID']],
                                                                               "endkey": [x['jobID'],
                                                                                          x['retry_count']],
                                                                               "stale" : "update_after"})['rows'][0]['value']['lfn']
                            logCollectID = self.jobsdatabase.loadView("JobDump", "jobsByInputLFN",
                                                                      options = {"startkey": [workflowName, logArchive],
                                                                                 "endkey": [workflowName, logArchive],
                                                                                 "stale" : "update_after"})['rows'][0]['value']
                            logCollect = self.fwjrdatabase.loadView("FWJRDump", "outputByJobID",
                                                                    options = {"startkey": logCollectID,
                                                                               "endkey": logCollectID,
                                                                               "stale" : "update_after"})['rows'][0]['value']['lfn']
                            x['logArchive'] = logArchive.split('/')[-1]
                            x['logCollect'] = logCollect
                        except IndexError as ex:
                            logging.debug("Unable to find final logArchive tarball for %i" % x['jobID'])
                            logging.debug(str(ex))
                        except KeyError as ex:
                            logging.debug("Unable to find final logArchive tarball for %i" % x['jobID'])
                            logging.debug(str(ex))


                    if key in self.histogramKeys:
                        # Usual histogram that was always done
                        histogram = MathAlgos.createHistogram(numList = output[key],
                                                              nBins = self.histogramBins,
                                                              limit = self.histogramLimit)
                        final[stepName][key]['histogram'] = histogram
                        # Histogram only picking values from failed jobs
                        # Operators  can use it to find out quicker why a workflow/task/step is failing :
                        if len(failedJobs) > 0:
                            failedJobsHistogram = MathAlgos.createHistogram(numList = outputFailed[key],
                                                                  nBins = self.histogramBins,
                                                                  limit = self.histogramLimit)
                                                        
                            final[stepName][key]['errorsHistogram'] = failedJobsHistogram
                    else:
                        average, stdDev = MathAlgos.getAverageStdDev(numList = output[key])
                        final[stepName][key]['average'] = average
                        final[stepName][key]['stdDev']  = stdDev

                    final[stepName][key]['worstOffenders'] = [{'jobID': x['jobID'], 'value': x.get(key, 0.0),
                                                               'log': x.get('logArchive', None),
                                                               'logCollect': x.get('logCollect', None)} for x in offenders]


            finalTask[taskName] = final
        return finalTask

    def getFailedJobs(self, workflowName):
        # We want ALL the jobs, and I'm sorry, CouchDB doesn't support wildcards, above-than-absurd values will do:
        errorView = self.fwjrdatabase.loadView("FWJRDump", "errorsByWorkflowName",
                                          options = {"startkey": [workflowName, 0, 0],
                                                     "endkey": [workflowName, 999999999, 999999],
                                                     "stale" : "update_after"})['rows']
        failedJobs = []
        for row in errorView:
            jobId = row['value']['jobid']
            if jobId not in failedJobs:
                failedJobs.append(jobId)
                
        return failedJobs

    def publishRecoPerfToDashBoard(self, workload):

        listRunsWorkflow = self.dbsDaoFactory(classname = "ListRunsWorkflow")
        
        interestingPDs = self.interestingPDs 
        interestingDatasets = []
        # Are the datasets from this request interesting? Do they have DQM output? One might ask afterwards if they have harvest
        for dataset in workload.listOutputDatasets():
            (nothing, PD, procDataSet, dataTier) = dataset.split('/')
            if PD in interestingPDs and dataTier == "DQM":
                interestingDatasets.append(dataset)
        # We should have found 1 interesting dataset at least
        logging.debug("Those datasets are interesting %s" % str(interestingDatasets))
        if len(interestingDatasets) == 0:
            return
        # Request will be only interesting for performance if it's a ReReco or PromptReco
        (isReReco, isPromptReco) = (False, False)
        if workload.getRequestType() == 'ReReco':
            isReReco = True
        # Yes, few people like magic strings, but have a look at :
        # https://github.com/dmwm/T0/blob/master/src/python/T0/RunConfig/RunConfigAPI.py#L718
        # Might be safe enough
        # FIXME: in TaskArchiver, add a test to make sure that the dataset makes sense (procDataset ~= /a/ERA-PromptReco-vVERSON/DQM)
        if re.search('PromptReco_', workload.name()):
            isPromptReco = True 
        if not (isReReco or isPromptReco):
            return        
        # We are not interested if it's not a PromptReco or a ReReco
        if (isReReco or isPromptReco) == False:
            return
        logging.info("%s has interesting performance information, trying to publish to DashBoard" % workload.name())
        release = workload.getCMSSWVersions()[0]
        if not release:
            logging.info("no release for %s, bailing out" % workload.name())

        
        # If all is true, get the run numbers processed by this worklfow        
        runList = listRunsWorkflow.execute(workflow = workload.name())
        # GO to DQM GUI, get what you want 
        for dataset in interestingDatasets :
            (nothing, PD, procDataSet, dataTier) = dataset.split('/')
            worthPoints = {}
            for run in runList :
                responseJSON = self.getPerformanceFromDQM(self.dqmUrl, dataset, run)
                if responseJSON:                
                    worthPoints.update(self.filterInterestingPerfPoints(responseJSON,
                                                                         self.perfDashBoardMinLumi,
                                                                         self.perfDashBoardMaxLumi))
                            
            # Publish dataset performance to DashBoard.
            if self.publishPerformanceDashBoard(self.dashBoardUrl, PD, release, worthPoints) == False:
                logging.info("something went wrong when publishing dataset %s to DashBoard" % dataset)
                
        return 

    
    def getPerformanceFromDQM(self, dqmUrl, dataset, run):
        
        # Get the proxy, as CMSWEB doesn't allow us to use plain HTTP
        hostCert = os.getenv("X509_HOST_CERT")
        hostKey  = os.getenv("X509_HOST_KEY")
        # it seems that curl -k works, but as we already have everything, I will just provide it
        
        # Make function to fetch this from DQM. Returning Null or False if it fails
        getUrl = "%sjsonfairy/archive/%s%s/DQM/TimerService/event_byluminosity" % (dqmUrl, run, dataset)
        logging.debug("Requesting performance information from %s" % getUrl)
        
        regExp = re.compile('https://(.*)(/dqm.+)')
        regExpResult = regExp.match(getUrl)
        dqmHost = regExpResult.group(1)
        dqmPath = regExpResult.group(2)
        
        connection = httplib.HTTPSConnection(dqmHost, 443, hostKey, hostCert)
        try:
            connection.request('GET', dqmPath)
            response = connection.getresponse()
            responseData = response.read()
            responseJSON = json.loads(responseData)
            if response.status != 200:
                logging.info("Something went wrong while fetching Reco performance from DQM, response code %d " % response.code)
                return False
        except Exception as ex:
            logging.error('Couldnt fetch DQM Performance data for dataset %s , Run %s' % (dataset, run))
            logging.exception(ex) #Let's print the stacktrace with generic Exception
            return False     

        try:
            if "content" in responseJSON["hist"]["bins"]:
                return responseJSON
        except Exception as ex:                    
            logging.info("Actually got a JSON from DQM perf in for %s run %d , but content was bad, Bailing out"
                         % (dataset, run))
            return False
        # If it gets here before returning False or responseJSON, it went wrong    
        return False
    
    def filterInterestingPerfPoints(self, responseJSON, minLumi, maxLumi):
        worthPoints = {}
        points = responseJSON["hist"]["bins"]["content"]
        for i in range(responseJSON["hist"]["xaxis"]["first"]["id"], responseJSON["hist"]["xaxis"]["last"]["id"]):
            # is the point worth it? if yes add to interesting points dictionary. 
            # 1 - non 0
            # 2 - between minimum and maximum expected luminosity
            # FIXME : 3 - population in dashboard for the bin interval < 100
            # Those should come from the config :
            if points[i] == 0:
                continue
            binSize = responseJSON["hist"]["xaxis"]["last"]["value"]/responseJSON["hist"]["xaxis"]["last"]["id"]
            # Fetching the important values
            instLuminosity = i*binSize 
            timePerEvent = points[i]
                    
            if instLuminosity > minLumi and instLuminosity < maxLumi:
                worthPoints[instLuminosity] = timePerEvent
        logging.debug("Got %d worthwhile performance points" % len(worthPoints.keys()))
        
        return worthPoints

    def publishPerformanceDashBoard(self, dashBoardUrl, PD, release, worthPoints):
        dashboardPayload = []
        for instLuminosity in worthPoints :
            timePerEvent = int(worthPoints[instLuminosity])
            dashboardPayload.append({"primaryDataset" : PD, 
                                     "release" : release, 
                                     "integratedLuminosity" : instLuminosity,
                                     "timePerEvent" : timePerEvent})
            
        data = "{\"data\":%s}" % str(dashboardPayload).replace("\'", "\"")
        headers = {"Accept":"application/json"}
        
        logging.debug("Going to upload this payload %s" % data)
        
        try:
            request = urllib2.Request(dashBoardUrl, data, headers)
            response = urllib2.urlopen(request)
            if response.code != 200:
                logging.info("Something went wrong while uploading to DashBoard, response code %d " % response.code)
                return False
        except Exception as ex:
            logging.error('Performance data : DashBoard upload failed for PD %s Release %s' % (PD, release))
            logging.exception(ex) #Let's print the stacktrace with generic Exception
            return False        

        logging.debug("Uploaded it successfully, apparently")        
        return True

    def createAndUploadPublish(self, workflow):
        """
        Upload to the UFC a JSON file with all the info needed to publish this dataset later
        """

        if self.uploadPublishDir:
            workDir = self.uploadPublishDir
        else:
            workDir, taskDir = getMasterName(startDir=self.jobCacheDir, workflow=workflow)

        try:
            return uploadPublishWorkflow(self.config, workflow, ufcEndpoint=self.userFileCacheURL, workDir=workDir)
        except Exception as ex:
            logging.error('Upload failed for workflow: %s' % (workflow))
            logging.exception(ex) #Let's print the stacktrace with generic Exception
            return False
