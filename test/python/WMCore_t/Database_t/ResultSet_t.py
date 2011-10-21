#!/usr/bin/env python
"""
_ResultSet_t_

Unit tests for the Database/ResultSet class

"""


#Written to test the ResultSet class initially by mnorman
#Dependent on DBCore, specifically DBCore.processData()

import unittest
import logging
import threading
import os

from WMCore.WMFactory import WMFactory
from WMCore.Database.ResultSet import ResultSet
from WMQuality.TestInit import TestInit
from sqlalchemy.engine.base import ResultProxy


class ResultSetTest(unittest.TestCase):


    def setUp(self):
        """
        Set up for initializing the ResultSet test class

        """

        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMQuality.TestDB"],
                                   useDefault = False)

        self.myThread = threading.currentThread()

        return

    def tearDown(self):
        """
        Delete the ResultSet test class

        """
        self.testInit.clearDatabase()
        return



    def testFullResultSet(self):
        """
        Function to test all the functionality of the ResultSet
        
        """
        
        #ResultSet is a very simple function.  This function should
        #create a ResultSet, fill it with data, and then read it
        #out, all in one go.

        
        testSet = ResultSet()
        
        testDict = {'column1': 'value1', 'column2':'value2'}
        binds    = [ {'column1':'value3a', 'column2': 'value2a'},
                     {'column1':'value3b', 'column2': 'value2b'},
                     {'column1':'value3c', 'column2': 'value2c'}]

       
        self.myThread.dbi.processData('insert into test_tablec (column1, column2) values (:column1, :column2)', testDict)
        self.myThread.dbi.processData('insert into test_tablec (column1, column2) values (:column1, :column2)', binds)
        testProxy = self.myThread.dbi.connection().execute('select * from test_tablec')
        
        #import pdb
        #pdb.set_trace()
        testSet.add(testProxy)
        self.assertEqual(testProxy.rowcount, 4)
        self.assertEqual(testSet.rowcount, 4)    
        #Test to make sure fetchone and fetchall both work
        self.assertEqual(str(testSet.fetchone()[0]), 'value1')
        self.assertEqual(str(testSet.fetchall()[-1][1]), 'value2c')

    def testRowCount(self):
        
        testSet = ResultSet()
        insertProxy = self.myThread.dbi.connection().execute("insert into test_tablec (column1, column2) values ('a', 'b')")
        testSet.add(insertProxy)
        self.assertEqual(testSet.rowcount, 1)    
        
        updateProxy = self.myThread.dbi.connection().execute("update test_tablec set column1 = 'c'")
        testSet.add(updateProxy)
        self.assertEqual(testSet.rowcount, 2)
        
        updateProxy = self.myThread.dbi.connection().execute("update test_tablec set column1 = 'c' where column1 = 'a'")
        testSet.add(updateProxy)
        self.assertEqual(testSet.rowcount, 2)
        
        insertProxy = self.myThread.dbi.connection().execute("insert into test_tablec (column1, column2) values ('a', 'b')")
        testSet.add(insertProxy)
        self.assertEqual(testSet.rowcount, 3)
        
        selectProxy = self.myThread.dbi.connection().execute("select * from test_tablec")
        testSet.add(selectProxy)
        self.assertEqual(testSet.rowcount, 5)
        
        selectProxy = self.myThread.dbi.connection().execute("delete from test_tablec")
        testSet.add(selectProxy)
        self.assertEqual(testSet.rowcount, 7)   
        
    def test1000Binds(self):

        testSet2 = ResultSet()

        #Now insert and select a huge number of values
        #This depends on the times variable
        binds = [ ]
        largeInsert  = [ ]
        times = 1000
                         
        #For now I don't want to get too dependent on processData() with many binds, since right now
        #it doesn't work.  That does make things awkward though.
        for i in range(times):
            binds = {'column1': 'value1'+str(i), 'column2':'value2'+str(i)}
            self.myThread.dbi.processData("insert into test_tablec (column1, column2) values (:column1, :column2)", binds)



        sql = "select column1 from test_tablec"
        testProxy = self.myThread.dbi.connection().execute(sql)

        testSet2.add(testProxy)

        self.assertEqual(len(testSet2.fetchall()), times)
        dialect = self.myThread.dialect.lower()
        if dialect == 'sqlite':
            self.assertEqual(str(testSet2.fetchall()[1][0]), 'value11')
            self.assertEqual(testSet2.fetchone()[0], None)
        elif dialect == 'mysql':
            self.assertEqual(str(testSet2.fetchall()[1][0]), 'value11')
            self.assertEqual(testSet2.fetchone()[0], 'value10')
        elif dialect == 'oracle':
            self.assertEqual(str(testSet2.fetchall()[1][0]), 'value11')
            self.assertEqual(testSet2.fetchone()[0], 'value10')
        

        return



if __name__ == "__main__":
    unittest.main()    
