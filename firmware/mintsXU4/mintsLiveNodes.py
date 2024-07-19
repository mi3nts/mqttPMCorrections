from cmath import nan
from datetime import datetime, timedelta
from os import name
import time
import random
import pandas as pd
#import pyqtgraph as pg
from collections import deque
#from pyqtgraph.Qt import QtGui, QtCore
from mintsXU4 import mintsSensorReader as mSR
from mintsXU4 import mintsLoRaReader as mLR
from mintsXU4 import mintsDefinitions as mD
from mintsXU4 import mintsProcessing as mP
from mintsXU4 import mintsLatest as mL
# from mintsXU4 import mintsNow as mN
import math
from geopy.geocoders import Nominatim

# from dateutil import tz
import numpy as np
#from pyqtgraph import AxisItem
from time import mktime
import statistics
from collections import OrderedDict
# import pytz
import sys


####SUMMARY - CLIMATE DATA READ - WORK ON PM DATA NEXT USING CORRECTIONS


# nodeIDs              = mD.mintsDefinitions['nodeIDs']
# airMarID             = mD.mintsDefinitions['airmarID']
# climateTargets       = mD.mintsDefinitions['climateTargets']
liveSpanSec            = mD.mintsDefinitions['liveSpanSec']

# rawPklsFolder        = mD.rawPklsFolder
# referencePklsFolder  = mD.referencePklsFolder
# mergedPklsFolder     = mD.mergedPklsFolder
# modelsPklsFolder     = mD.modelsPklsFolder
liveFolder           = mD.liveFolder

class node:
    def __init__(self,nodeInfoRow):
        self.nodeID = nodeInfoRow['nodeID']
        print("============MINTS============")
        print("NODE ID: " + self.nodeID)

        self.pmSensor      = nodeInfoRow['pmSensor']
        self.climateSensor = nodeInfoRow['climateSensor']
        
        self.climateNullValidity     = 0
        self.climateDateTimeValidity = 0
        self.climateValidity         = 0
        self.humidityValidity        = 0
        self.dewPointValidity        = 0 
        self.mlValidity              = 0


        self.PMDateTime      = datetime(2010, 1, 1, 0, 0, 0, 0)
        self.climateDateTime = datetime(2010, 1, 1, 0, 0, 0, 0)

        self.pc0_1          = 0
        self.pc0_3          = 0
        self.pc0_5          = 0
        self.pc1_0          = 0
        self.pc2_5          = 0
        self.pc5_0          = 0
        self.pc10_0         = 0

        self.pm0_1          = 0.0
        self.pm0_3          = 0.0
        self.pm0_5          = 0.0
        self.pm1_0          = 0.0
        self.pm2_5          = 0.0
        self.pm5_0          = 0.0
        self.pm10_0         = 0.0
        

        #  Corrected PC Values 
        self.cor_pc0_1      = 0
        self.cor_pc0_3      = 0
        self.cor_pc0_5      = 0
        self.cor_pc1_0      = 0
        self.cor_pc2_5      = 0
        self.cor_pc5_0      = 0
        self.cor_pc10_0     = 0


        #  Corrected PM Values 
        self.cor_pm0_1      = 0.0
        self.cor_pm0_3      = 0.0
        self.cor_pm0_5      = 0.0
        self.cor_pm1_0      = 0.0
        self.cor_pm2_5      = 0.0
        self.cor_pm5_0      = 0.0
        self.cor_pm10_0     = 0.0
        self.ml_pm2_5       = 0.0

        # if self.climateSensor  in {"BME280", "BME680", "BME688CNR"}:
        self.temperature         = []
        self.pressure            = []
        self.humidity            = []
        self.dewPoint            = []



    def update(self,sensorID,sensorDictionary):
        if sensorID == self.pmSensor:
             self.nodeReaderPM(sensorDictionary)                
        if sensorID == self.climateSensor:
            self.nodeReaderClimate(sensorDictionary)



    def nodeReaderPM(self,jsonData):
        # if (True):
        try:
            # print(jsonData)
            self.dataInPM       = jsonData
            self.ctNowPM        = datetime.strptime(self.dataInPM['dateTime'],'%Y-%m-%d %H:%M:%S.%f')
            # print(self.ctNowPM)

            if (self.ctNowPM>self.lastPMDateTime):
                self.currentUpdatePM()
        except Exception as e:
            print("[ERROR] Could not read JSON data, error: {}".format(e))



    def currentUpdatePM(self):
        # print("PM Data Read")
        # print(self.dataInPM)
        # At this point I need to consider the sensor, 
        # but since both the IPS7100 and the IPS7100 has the
        # same outputs, there is no need 
        if self.pmSensor  in {"IPS7100", "IPS7100CNR"}:
            self.pc0_1.append(float(self.dataInPM['pc0_1']))
            self.pc0_3.append(float(self.dataInPM['pc0_3']))
            self.pc0_5.append(float(self.dataInPM['pc0_5']))
            self.pc1_0.append(float(self.dataInPM['pc1_0']))
            self.pc2_5.append(float(self.dataInPM['pc2_5']))
            self.pc5_0.append(float(self.dataInPM['pc5_0']))
            self.pc10_0.append(float(self.dataInPM['pc10_0']))
            self.pm0_1.append(float(self.dataInPM['pm0_1']))
            self.pm0_3.append(float(self.dataInPM['pm0_3']))
            self.pm0_5.append(float(self.dataInPM['pm0_5']))
            self.pm1_0.append(float(self.dataInPM['pm1_0']))
            self.pm2_5.append(float(self.dataInPM['pm2_5']))
            self.pm5_0.append(float(self.dataInPM['pm5_0']))
            self.pm10_0.append(float(self.dataInPM['pm10_0']))
            timeIn = datetime.strptime(self.dataInPM['dateTime'],'%Y-%m-%d %H:%M:%S.%f')
            self.dateTimePM.append(timeIn)
            self.lastPMDateTime = timeIn


    def calculateDewPointInC(self,temperature, humidity):
        dewPoint = 243.04 * (math.log(humidity/100.0) + ((17.625 * temperature)/(243.04 + temperature))) / (17.625 - math.log(humidity/100.0) - ((17.625 * temperature)/(243.04 + temperature)))
        return dewPoint



    def nodeReaderClimate(self,jsonData):
        try:
            self.dataInClimate  = jsonData
            self.ctNowClimate   = datetime.strptime(self.dataInClimate['dateTime'],'%Y-%m-%d %H:%M:%S.%f')
            if (self.ctNowClimate>self.lastClimateDateTime):
                self.currentUpdateClimate()
        except Exception as e:
            print("[ERROR] Could not read JSON data, error: {}".format(e))
    
    def currentUpdateClimate(self):

        # The climate data should have the following units 
            # temperature : C 
            # humidity    : %
            # pressure    : miliBars
            # dewPoint    : C  
            
        if self.climateSensor in {"BME280V2"}:        
            self.temperature = float(self.dataInClimate['temperature'])
            self.pressure    = float(self.dataInClimate['pressure'])
            self.humidity    = float(self.dataInClimate['humidity'])
            self.dewPoint    = float(self.dataInClimate['dewPoint'])
            self.climateDateTime  = datetime.strptime(self.dataInClimate['dateTime'],'%Y-%m-%d %H:%M:%S.%f')

        if self.climateSensor in {"WIMDA"}:    
            self.temperature    = float(self.dataInClimate['airTemperature'])
            self.pressure       = float(self.dataInClimate['barrometricPressureBars'])*1000
            self.humidity       = float(self.dataInClimate['relativeHumidity'])
            self.dewPoint       = float(self.dataInClimate['dewPoint'])
            self.climateDateTime  = datetime.strptime(self.dataInClimate['dateTime'],'%Y-%m-%d %H:%M:%S.%f')

        if self.climateSensor in {"BME280"}:       
            self.temperature      = float(self.dataInClimate['temperature'])
            self.pressure         = float(self.dataInClimate['pressure'])/100
            self.humidity         = float(self.dataInClimate['humidity'])
            self.dewPoint         = float(self.calculateDewPointInC(self.temperature, self.humidity))
            self.climateDateTime  = datetime.strptime(self.dataInClimate['dateTime'],'%Y-%m-%d %H:%M:%S.%f')

        if self.climateSensor in {"BME680"}:       
            self.temperature      = float(self.dataInClimate['temperature'])
            self.pressure         = float(self.dataInClimate['pressure'])*10
            self.humidity         = float(self.dataInClimate['humidity'])
            self.dewPoint         = float(self.calculateDewPointInC(self.temperature, self.humidity))
            self.climateDateTime  = datetime.strptime(self.dataInClimate['dateTime'],'%Y-%m-%d %H:%M:%S.%f')

        if self.climateSensor in {"BME688CNR"}:       
            self.temperature      = float(self.dataInClimate['temperature'])
            self.pressure         = float(self.dataInClimate['pressure'])
            self.humidity         = float(self.dataInClimate['humidity'])
            self.dewPoint         = float(self.calculateDewPointInC(self.temperature, self.humidity))
            self.climateDateTime  = datetime.strptime(self.dataInClimate['dateTime'],'%Y-%m-%d %H:%M:%S.%f')




    def getValidity(self):
        # print("Getting Validity")     
        return len(self.pm0_1)>=1;

    def doCSV(self):

        # sensorDictionary = OrderedDict([
            # ("dateTime"                 ,str(dateTime)), # always the same
            # ("pc0_1"                    ,round(cor_pc0_1)), 
            # ("pc0_3"                    ,round(cor_pc0_3)),
            # ("pc0_5"                    ,round(cor_pc0_5)),
            # ("pc1_0"                    ,round(cor_pc1_0)),
            # ("pc2_5"                    ,round(cor_pc2_5)),
            # ("pc5_0"                    ,round(cor_pc5_0)), 
            # ("pc10_0"                   ,round(cor_pc10_0)),
            # ("pm0_1"                    ,cor_pm0_1), 
            # ("pm0_3"                    ,cor_pm0_3),
            # ("pm0_5"                    ,cor_pm0_5),
            # ("pm1_0"                    ,cor_pm1_0),
            # ("pm2_5"                    ,cor_pm2_5),
            # ("pm5_0"                    ,cor_pm5_0), 
            # ("pm10_0"                   ,cor_pm10_0),
            # ("pm2_5ML"                  ,ml_pm2_5),
            # ("temperature"              ,temperature),
            # ("pressure"                 ,pressure), 
            # ("humidity"                 ,humidity),
            # ("dewPoint"                 ,dewPoint),
            # ("climateNullValidity"      ,climateNullValidity), 
            # ("climateDateTimeValidity"  ,climateDateTimeValidity),
            # ("climateValidity"          ,climateValidity),
            # ("humidityValidity"         ,humidityValidity),
            # ("dewPointValidity"         ,dewPointValidity),
            # ("mlValidity"               ,mlValidity)
            # ])
        # print(sensorDictionary)


      
        print()        
        print("===============MINTS===============")
        # print(sensorDictionary)

        # ADJUST THE SENSOR ID HERE
        # mP.writeCSV3( mP.getWritePathDateCSV(liveFolder,self.nodeID,\
        #     datetime.strptime(self.dateTimeStrCSV,'%Y-%m-%d %H:%M:%S.%f'),\
        #         "calibrated"),sensorDictionary)
        print("CSV Written")
        # mL.writeMQTTLatestRepublish(sensorDictionary,"mintsCalibrated",self.nodeID)





    




