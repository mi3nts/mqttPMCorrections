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
# liveSpanSec            = mD.mintsDefinitions['liveSpanSec']

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

        self.pmDateTime      = datetime(2010, 1, 1, 0, 0, 0, 0)
        self.climateDateTime = datetime(2010, 1, 1, 0, 0, 0, 0)

        self.pc0_1          = -100
        self.pc0_3          = -100
        self.pc0_5          = -100
        self.pc1_0          = -100
        self.pc2_5          = -100
        self.pc5_0          = -100
        self.pc10_0         = -100

        self.pm0_1          = -100
        self.pm0_3          = -100
        self.pm0_5          = -100
        self.pm1_0          = -100
        self.pm2_5          = -100
        self.pm5_0          = -100
        self.pm10_0         = -100
        
        #  Corrected PC Values 
        self.cor_pc0_1      = -100
        self.cor_pc0_3      = -100
        self.cor_pc0_5      = -100
        self.cor_pc1_0      = -100
        self.cor_pc2_5      = -100
        self.cor_pc5_0      = -100
        self.cor_pc10_0     = -100

        #  Corrected PM Values 
        self.cor_pm0_1      = -100
        self.cor_pm0_3      = -100
        self.cor_pm0_5      = -100
        self.cor_pm1_0      = -100
        self.cor_pm2_5      = -100
        self.cor_pm5_0      = -100
        self.cor_pm10_0     = -100
        # self.ml_pm2_5       = 0

        # if self.climateSensor  in {"BME280", "BME680", "BME688CNR"}:
        self.temperature         = -100
        self.pressure            = -100
        self.humidity            = -100
        self.dewPoint            = -100

        # Validity Variables 
        self.temperatureValidity        = 0 # Checks if temeperature readings are in range 
        self.humidityValidity           = 0 # Checks if humidity readings are in range 
        self.momentaryValidity          = 0 # Checks if climate readings are reasont 
        self.humidityLikelyhoodValidity = 0 # Checks if humdity readings make sense for fog to be created 
        self.dewPointValidity           = 0 # Checks if temperature and dew point is close enough to make sense for fog to be create
        self.correctionRequirment       = 0 # Master  check 
         
    def update(self,sensorID,sensorDictionary):
        if sensorID == self.pmSensor:
            print(" - - - PM SENSOR FOUND - - - ")
            print(sensorDictionary)
            self.nodeReaderPM(sensorDictionary)                
        if sensorID == self.climateSensor:
            print(" - - - CLIMATE SENSOR FOUND - - - ")
            self.nodeReaderClimate(sensorDictionary)



    def nodeReaderPM(self,jsonData):
        if (True):
        # try:
            # print(jsonData)
            self.dataInPM       = jsonData
            self.ctNowPM        = datetime.strptime(self.dataInPM['dateTime'],'%Y-%m-%d %H:%M:%S.%f')
            # print(self.ctNowPM)

            if (self.ctNowPM>self.pmDateTime):
                self.currentUpdatePM()
        # except Exception as e:
        #     print("[ERROR] Could not read JSON data, error: {}".format(e))



    def currentUpdatePM(self):
        # print("PM Data Read")
        # print(self.dataInPM)
        # At this point I need to consider the sensor, 
        # but since both the IPS7100 and the IPS7100 has the
        # same outputs, there is no need 

        if self.pmSensor  in {"IPS7100", "IPS7100CNR"}:
            
            self.pc0_1  = self.dataInPM['pc0_1']
            self.pc0_3  = self.dataInPM['pc0_3'] 
            self.pc0_5  = self.dataInPM['pc0_5'] 
            self.pc1_0  = self.dataInPM['pc1_0'] 
            self.pc2_5  = self.dataInPM['pc2_5'] 
            self.pc5_0  = self.dataInPM['pc5_0'] 
            self.pc10_0 = self.dataInPM['pc10_0']

            self.pm0_1  = self.dataInPM['pm0_1'] 
            self.pm0_3  = self.dataInPM['pm0_3'] 
            self.pm0_5  = self.dataInPM['pm0_5'] 
            self.pm1_0  = self.dataInPM['pm1_0'] 
            self.pm2_5  = self.dataInPM['pm2_5'] 
            self.pm5_0  = self.dataInPM['pm5_0'] 
            self.pm10_0 = self.dataInPM['pm10_0'] 
            
            self.pmDateTime = datetime.strptime(self.dataInPM['dateTime'],'%Y-%m-%d %H:%M:%S.%f')

            self.cor_pc0_1, self.cor_pc0_3, self.cor_pc0_5, \
                self.cor_pc1_0, self.cor_pc2_5, self.cor_pc5_0,\
                    self.cor_pc10_0 \
                                =  float(self.pc0_1), float(self.pc0_3), float(self.pc0_5),\
                                    float(self.pc1_0), float(self.pc2_5), float(self.pc5_0), \
                                        float(self.pc10_0)        
                 
            self.cor_pm0_1, self.cor_pm0_3, self.cor_pm0_5, \
                self.cor_pm1_0, self.cor_pm2_5, self.cor_pm5_0,\
                    self.cor_pm10_0 \
                                =  self.pm0_1, self.pm0_3, self.pm0_5,\
                                    self.pm1_0, self.pm2_5, self.pm5_0, \
                                        self.pm10_0 

            # Check if conditions are met for fog to be generated 
            self.getValidity()

            if self.correctionRequirment:
                # At this point - apply the corrections
                print("Applying corrections") 
                self.humidityCorrectedPC()
                self.humidityCorrectedPM()
   





    def is_valid_temperature(self,temp):
        return -20 <= temp <= 50  # Assuming temperature is in Celsius

    def is_valid_humidity(self,humidity):
        return 0 <= humidity <= 100  # Assuming humidity is in percentage

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
        print("Checking Validity")

        if self.is_valid_temperature(self.temperature):
            print("Temperatuere is valid")
            self.temperatureValidity = 1
        else:
            self.temperatureValidity = 0

        if self.is_valid_humidity(self.humidity):
            print("Humidity is valid")
            self.humidityValidity = 1
        else:
            self.humidityValidity = 0

        if (self.pmDateTime -self.climateDateTime).total_seconds() < 300: 
            print("Climate date time is closer")
            self.momentaryValidity = 1
        else:
            self.momentaryValidity = 0

        if self.humidity > 40:
            print("Humidity data is valid")
            self.humidityLikelyhoodValidity = 1
        else:
            self.humidityLikelyhoodValidity = 0

        T_D = self.temperature - self.dewPoint
        if T_D < 2.5:
            print("Dewpint data is valid")
            self.dewPointValidity = 1
        else: 
            self.dewPointValidity = 0

        self.correctionRequirment =  self.temperatureValidity and self.humidityValidity and self.momentaryValidity \
                    and self.humidityLikelyhoodValidity and self.dewPointValidity

    def humidityCorrectedPC(self):

        pc0_1  = float(self.pc0_1)
        pc0_3  = float(self.pc0_3)
        pc0_5  = float(self.pc0_5)
        pc1_0  = float(self.pc1_0)
        pc2_5  = float(self.pc2_5)
        pc5_0  = float(self.pc5_0)
        pc10_0 = float(self.pc10_0)

        hum = float(self.humidity)
        # tem = float(self.temperature)
        # dew = float(self.dewPoint)

        print('Condition is satisfied')
        data = {'count': [pc0_1, None, pc0_3, pc0_5, pc1_0, pc2_5, pc5_0, pc10_0, None],
                'D_range': [50, 20, 200, 200, 500, 1500, 2500, 5000, None],
                'D_point': [50, 80, 100, 300, 500, 1000, 2500, 5000, 10000]}
        df1 = pd.DataFrame(data)
        df1['N/D'] = df1['count']/df1['D_range']

        df1['height_ini'] = 0
        df1.loc[7, 'height_ini'] = (2*df1.loc[7, 'count'])/5000
        df1.loc[6, 'height_ini'] = (2*df1.loc[6, 'count'])/2500 - df1.loc[7, 'height_ini']
        df1.loc[5, 'height_ini'] = (2*df1.loc[5, 'count'])/1500 - df1.loc[6, 'height_ini']
        df1.loc[4, 'height_ini'] = (2*df1.loc[4, 'count'])/500 - df1.loc[5, 'height_ini']
        df1.loc[3, 'height_ini'] = (2*df1.loc[3, 'count'])/200 - df1.loc[4, 'height_ini']
        df1.loc[2, 'height_ini'] = (2*df1.loc[2, 'count'])/200 - df1.loc[3, 'height_ini']
        df1.loc[0, 'height_ini'] = (2*df1.loc[0, 'count'])/50 - df1.loc[2, 'height_ini']
        df1.loc[1, 'height_ini'] = (20*(df1.loc[0, 'height_ini']-df1.loc[2, 'height_ini'])/50) + df1.loc[2, 'height_ini']
        df1.loc[1, 'count'] = 0.5*(df1.loc[1, 'height_ini']+df1.loc[2, 'height_ini'])*20

        RH = (hum) * 0.7
        RH = 98 if RH >= 99 else RH
        k = 0.62
        df1['D_dry_point'] = df1['D_point']/((1 + k*(RH/(100-RH)))**(1/3))

        df1['D_dry_range'] = df1['D_dry_point'].diff().shift(-1)

        df1['fit_height_ini'] = 0

        df1.loc[7, 'fit_height_ini'] = (2*df1.loc[7, 'count'])/df1.loc[7, 'D_dry_range']
        df1.loc[6, 'fit_height_ini'] = (2*df1.loc[6, 'count'])/df1.loc[6, 'D_dry_range'] - df1.loc[7, 'fit_height_ini']
        df1.loc[5, 'fit_height_ini'] = (2*df1.loc[5, 'count'])/df1.loc[5, 'D_dry_range'] - df1.loc[6, 'fit_height_ini']
        df1.loc[4, 'fit_height_ini'] = (2*df1.loc[4, 'count'])/df1.loc[4, 'D_dry_range'] - df1.loc[5, 'fit_height_ini']
        df1.loc[3, 'fit_height_ini'] = (2*df1.loc[3, 'count'])/df1.loc[3, 'D_dry_range'] - df1.loc[4, 'fit_height_ini']
        df1.loc[2, 'fit_height_ini'] = (2*df1.loc[2, 'count'])/df1.loc[2, 'D_dry_range'] - df1.loc[3, 'fit_height_ini']
        df1.loc[1, 'fit_height_ini'] = (2*df1.loc[1, 'count'])/df1.loc[1, 'D_dry_range'] - df1.loc[2, 'fit_height_ini']

        df1['slope'] = (df1['fit_height_ini'].shift(-1) - df1['fit_height_ini']) / df1['D_dry_range']
        df1['interc'] = df1['fit_height_ini'] - df1['slope'] * df1['D_dry_point']

        df1['cor_height'] = None
        df1['cor_count'] = 0

        if df1.loc[8, 'D_dry_point'] > 5000:
            df1.loc[7, 'cor_height'] = df1.loc[7, 'slope']*5000 + df1.loc[7, 'interc']
            df1.loc[7, 'cor_count'] = 0.5*df1.loc[7, 'cor_height']*(df1.loc[8, 'D_dry_point']-5000)
        else:
            df1.loc[7, 'cor_height'] = 0
            df1.loc[7, 'cor_count'] = 0
        
        if (2500<df1.loc[7, 'D_dry_point']<=5000)&(df1.loc[8, 'D_dry_point']>5000):
            df1.loc[6, 'cor_height'] = df1.loc[6, 'slope']*2500 + df1.loc[6, 'interc']
            df1.loc[6, 'cor_count'] = (0.5*(df1.loc[7, 'cor_height']+df1.loc[7, 'fit_height_ini'])*(5000-df1.loc[7, 'D_dry_point'])) + (0.5*(df1.loc[6, 'cor_height']+df1.loc[7, 'fit_height_ini'])*(df1.loc[7, 'D_dry_point']-2500))
        elif (2500<df1.loc[7, 'D_dry_point']<5000)&(df1.loc[8, 'D_dry_point']<5000):
            df1.loc[6, 'cor_height'] = df1.loc[6, 'slope']*2500 + df1.loc[6, 'interc']
            df1.loc[6, 'cor_count'] = (0.5*(df1.loc[6, 'cor_height']+df1.loc[7, 'fit_height_ini'])*(df1.loc[7, 'D_dry_point']-2500)) + (0.5*df1.loc[7, 'fit_height_ini']*(df1.loc[8, 'D_dry_point']-df1.loc[7, 'D_dry_point']))
        elif (df1.loc[7, 'D_dry_point']<2500)&(df1.loc[8, 'D_dry_point']<5000):
            df1.loc[6, 'cor_height'] = df1.loc[7, 'slope']*2500 + df1.loc[7, 'interc']
            df1.loc[6, 'cor_count'] = (0.5*df1.loc[6, 'cor_height'])*(df1.loc[8, 'D_dry_point']-2500)
        else:
            df1.loc[6, 'cor_height'] = df1.loc[7, 'slope']*2500 + df1.loc[7, 'interc']
            df1.loc[6, 'cor_count'] = 0.5*(df1.loc[7, 'cor_height']+df1.loc[6, 'cor_height'])*2500
        
        if (1000<df1.loc[6, 'D_dry_point']<=2500)&(df1.loc[7, 'D_dry_point']>2500):
            df1.loc[5, 'cor_height'] = df1.loc[5, 'slope']*1000 + df1.loc[5, 'interc']
            df1.loc[5, 'cor_count'] = (0.5*(df1.loc[6, 'cor_height']+df1.loc[6, 'fit_height_ini'])*(2500-df1.loc[6, 'D_dry_point'])) + (0.5*(df1.loc[5, 'cor_height']+df1.loc[6, 'fit_height_ini'])*(df1.loc[6, 'D_dry_point']-1000))
        elif (1000<df1.loc[6, 'D_dry_point']<2500)&(df1.loc[7, 'D_dry_point']<2500):
            df1.loc[5, 'cor_height'] = df1.loc[5, 'slope']*1000 + df1.loc[5, 'interc']
            df1.loc[5, 'cor_count'] = (0.5*(df1.loc[5, 'cor_height']+df1.loc[6, 'fit_height_ini'])*(df1.loc[6, 'D_dry_point']-1000)) + (0.5*(df1.loc[6,'fit_height_ini']+df1.loc[7,'fit_height_ini'])*(df1.loc[7,'D_dry_point']-df1.loc[6,'D_dry_point'])) + (0.5*(df1.loc[7,'fit_height_ini']+df1.loc[6,'cor_height'])*(2500-df1.loc[7,'D_dry_point']))
        elif (df1.loc[6,'D_dry_point']<1000)&(df1.loc[7,'D_dry_point']<2500):
            df1.loc[5,'cor_height'] = df1.loc[6,'slope']*1000 + df1.loc[6,'interc']
            df1.loc[5,'cor_count'] = (0.5*(df1.loc[6,'cor_height']+df1.loc[7,'fit_height_ini'])*(2500-df1.loc[7,'D_dry_point'])) + (0.5*(df1.loc[5,'cor_height']+df1.loc[7,'fit_height_ini'])*(df1.loc[7,'D_dry_point']-1000))
        else:
            df1.loc[5,'cor_height'] = df1.loc[6,'slope']*1000 + df1.loc[6,'interc']
            df1.loc[5,'cor_count'] = 0.5*(df1.loc[6,'cor_height']+df1.loc[5,'cor_height'])*1500

        if (500<df1.loc[5,'D_dry_point']<=1000)&(df1.loc[6,'D_dry_point']>1000):
            df1.loc[4,'cor_height'] = df1.loc[4,'slope']*500 + df1.loc[4,'interc']
            df1.loc[4,'cor_count'] = (0.5*(df1.loc[5,'cor_height']+df1.loc[5,'fit_height_ini'])*(1000-df1.loc[5,'D_dry_point'])) + (0.5*(df1.loc[4,'cor_height']+df1.loc[5,'fit_height_ini'])*(df1.loc[5,'D_dry_point']-500))
        elif (500<df1.loc[5,'D_dry_point']<1000)&(df1.loc[6,'D_dry_point']<1000):
            df1.loc[4,'cor_height'] = df1.loc[4,'slope']*500 + df1.loc[4,'interc']
            df1.loc[4,'cor_count'] = (0.5*(df1.loc[4,'cor_height']+df1.loc[5,'fit_height_ini'])*(df1.loc[5,'D_dry_point']-500)) + (0.5*(df1.loc[5,'fit_height_ini']+df1.loc[6,'fit_height_ini'])*(df1.loc[6,'D_dry_point']-df1.loc[5,'D_dry_point'])) + (0.5*(df1.loc[6,'fit_height_ini']+df1.loc[5,'cor_height'])*(1000-df1.loc[6,'D_dry_point']))
        elif (df1.loc[5,'D_dry_point']<500)&(df1.loc[6,'D_dry_point']<1000):
            df1.loc[4,'cor_height'] = df1.loc[5,'slope']*500 + df1.loc[5,'interc']
            df1.loc[4,'cor_count'] = (0.5*(df1.loc[5,'cor_height']+df1.loc[6,'fit_height_ini'])*(1000-df1.loc[6,'D_dry_point'])) + (0.5*(df1.loc[4,'cor_height']+df1.loc[6,'fit_height_ini'])*(df1.loc[6,'D_dry_point']-500))
        else:
            df1.loc[4,'cor_height'] = df1.loc[5,'slope']*500 + df1.loc[5,'interc']
            df1.loc[4,'cor_count'] = 0.5*(df1.loc[5,'cor_height']+df1.loc[4,'cor_height'])*500

        if (300<df1.loc[4,'D_dry_point']<=500)&(df1.loc[5,'D_dry_point']>500):
            df1.loc[3,'cor_height'] = df1.loc[3,'slope']*300 + df1.loc[3,'interc']
            df1.loc[3,'cor_count'] = (0.5*(df1.loc[4,'cor_height']+df1.loc[4,'fit_height_ini'])*(500-df1.loc[4,'D_dry_point'])) + (0.5*(df1.loc[3,'cor_height']+df1.loc[4,'fit_height_ini'])*(df1.loc[4,'D_dry_point']-300))
        elif (300<df1.loc[4,'D_dry_point']<500)&(df1.loc[5,'D_dry_point']<500):
            df1.loc[3,'cor_height'] = df1.loc[3,'slope']*300 + df1.loc[3,'interc']
            df1.loc[3,'cor_count'] = (0.5*(df1.loc[3,'cor_height']+df1.loc[4,'fit_height_ini'])*(df1.loc[4,'D_dry_point']-300)) + (0.5*(df1.loc[4,'fit_height_ini']+df1.loc[5,'fit_height_ini'])*(df1.loc[5,'D_dry_point']-df1.loc[4,'D_dry_point'])) + (0.5*(df1.loc[5,'fit_height_ini']+df1.loc[4,'cor_height'])*(500-df1.loc[5,'D_dry_point']))
        elif (df1.loc[4,'D_dry_point']<300)&(df1.loc[5,'D_dry_point']<500):
            df1.loc[3,'cor_height'] = df1.loc[4,'slope']*300 + df1.loc[4,'interc']
            df1.loc[3,'cor_count'] = (0.5*(df1.loc[4,'cor_height']+df1.loc[5,'fit_height_ini'])*(500-df1.loc[5,'D_dry_point'])) + (0.5*(df1.loc[3,'cor_height']+df1.loc[5,'fit_height_ini'])*(df1.loc[5,'D_dry_point']-300))
        else:
            df1.loc[3,'cor_height'] = df1.loc[4,'slope']*300 + df1.loc[4,'interc']
            df1.loc[3,'cor_count'] = 0.5*(df1.loc[4,'cor_height']+df1.loc[3,'cor_height'])*200

        if (100<df1.loc[3,'D_dry_point']<=300)&(df1.loc[4,'D_dry_point']>300):
            df1.loc[2,'cor_height'] = df1.loc[2,'slope']*100 + df1.loc[2,'interc']
            df1.loc[2,'cor_count'] = (0.5*(df1.loc[3,'cor_height']+df1.loc[3,'fit_height_ini'])*(300-df1.loc[3,'D_dry_point'])) + (0.5*(df1.loc[2,'cor_height']+df1.loc[3,'fit_height_ini'])*(df1.loc[3,'D_dry_point']-100))
        elif (100<df1.loc[3,'D_dry_point']<300)&(df1.loc[4,'D_dry_point']<300):
            df1.loc[2,'cor_height'] = df1.loc[2,'slope']*100 + df1.loc[2,'interc']
            df1.loc[2,'cor_count'] = (0.5*(df1.loc[2,'cor_height']+df1.loc[3,'fit_height_ini'])*(df1.loc[3,'D_dry_point']-100)) + (0.5*(df1.loc[3,'fit_height_ini']+df1.loc[4,'fit_height_ini'])*(df1.loc[4,'D_dry_point']-df1.loc[3,'D_dry_point'])) + (0.5*(df1.loc[4,'fit_height_ini']+df1.loc[3,'cor_height'])*(300-df1.loc[4,'D_dry_point']))
        elif (df1.loc[3,'D_dry_point']<100)&(df1.loc[4,'D_dry_point']<300):
            df1.loc[2,'cor_height'] = df1.loc[3,'slope']*100 + df1.loc[3,'interc']
            df1.loc[2,'cor_count'] = (0.5*(df1.loc[3,'cor_height']+df1.loc[4,'fit_height_ini'])*(300-df1.loc[4,'D_dry_point'])) + (0.5*(df1.loc[2,'cor_height']+df1.loc[4,'fit_height_ini'])*(df1.loc[4,'D_dry_point']-100))
        else:
            df1.loc[2,'cor_height'] = df1.loc[3,'slope']*100 + df1.loc[3,'interc']
            df1.loc[2,'cor_count'] = 0.5*(df1.loc[3,'cor_height']+df1.loc[2,'cor_height'])*200

        if (50<df1.loc[2,'D_dry_point']<=100)&(df1.loc[3,'D_dry_point']>100):
            df1.loc[0,'cor_height'] = df1.loc[1,'slope']*50 + df1.loc[1,'interc']
            df1.loc[0,'cor_count'] = (0.5*(df1.loc[2,'cor_height']+df1.loc[2,'fit_height_ini'])*(100-df1.loc[2,'D_dry_point'])) + (0.5*(df1.loc[0,'cor_height']+df1.loc[2,'fit_height_ini'])*(df1.loc[2,'D_dry_point']-50))
        elif (50<df1.loc[2,'D_dry_point']<100)&(df1.loc[3,'D_dry_point']>100):
            df1.loc[0,'cor_height'] = df1.loc[1,'slope']*50 + df1.loc[1,'interc']
            df1.loc[0,'cor_count'] = (0.5*(df1.loc[0,'cor_height']+df1.loc[2,'fit_height_ini'])*(df1.loc[2,'D_dry_point']-50)) + (0.5*(df1.loc[2,'fit_height_ini']+df1.loc[3,'fit_height_ini'])*(df1.loc[3,'D_dry_point']-df1.loc[2,'D_dry_point'])) + (0.5*(df1.loc[3,'fit_height_ini']+df1.loc[2,'cor_height'])*(100-df1.loc[3,'D_dry_point']))
        elif (df1.loc[2,'D_dry_point']<50)&(df1.loc[3,'D_dry_point']>100):
            df1.loc[0,'cor_height'] = df1.loc[2,'slope']*50 + df1.loc[2,'interc']
            df1.loc[0,'cor_count'] = (0.5*(df1.loc[2,'cor_height']+df1.loc[3,'fit_height_ini'])*(100-df1.loc[3,'D_dry_point'])) + (0.5*(df1.loc[0,'cor_height']+df1.loc[3,'fit_height_ini'])*(df1.loc[3,'D_dry_point']-50))
        else:
            df1.loc[0,'cor_height'] = df1.loc[2,'slope']*50 + df1.loc[2,'interc']
            df1.loc[0,'cor_count'] = 0.5*(df1.loc[2,'cor_height']+df1.loc[0,'cor_height'])*50
            
        
        self.cor_pc0_1, self.cor_pc0_3, self.cor_pc0_5, self.cor_pc1_0, self.cor_pc2_5, self.cor_pc5_0, self.cor_pc10_0 = \
            df1.loc[0,'cor_count'], df1.loc[2,'cor_count'], df1.loc[3,'cor_count'], df1.loc[4,'cor_count'], df1.loc[5,'cor_count'], df1.loc[6,'cor_count'], df1.loc[7,'cor_count']
        

        
    def humidityCorrectedPM(self):

        m0_1 = 8.355696123812269e-07
        m0_3 = 2.2560825222215327e-05
        m0_5 = 0.00010446111749483851
        m1_0 = 0.0008397941861044865
        m2_5 = 0.013925696906339288
        m5_0 = 0.12597702778750686
        m10_0 = 1.0472

        self.cor_pm0_1   = m0_1*self.cor_pc0_1
        self.cor_pm0_3   = self.cor_pm0_1 + m0_3*self.cor_pc0_3
        self.cor_pm0_5   = self.cor_pm0_3 + m0_5*self.cor_pc0_5
        self.cor_pm1_0   = self.cor_pm0_5 + m1_0*self.cor_pc1_0
        self.cor_pm2_5   = self.cor_pm1_0 + m2_5*self.cor_pc2_5
        self.cor_pm5_0   = self.cor_pm2_5 + m5_0*self.cor_pc5_0
        self.cor_pm10_0  = self.cor_pm5_0 + m10_0*self.cor_pc10_0

        print("Humidity Corrected PM")


    def doCSV(self):

        sensorDictionary = OrderedDict([
            ("dateTime"                     ,str(self.pmDateTime)), # always the same
            ("pc0_1"                        ,round(self.cor_pc0_1)), 
            ("pc0_3"                        ,round(self.cor_pc0_3)),
            ("pc0_5"                        ,round(self.cor_pc0_5)),
            ("pc1_0"                        ,round(self.cor_pc1_0)),
            ("pc2_5"                        ,round(self.cor_pc2_5)),
            ("pc5_0"                        ,round(self.cor_pc5_0)), 
            ("pc10_0"                       ,round(self.cor_pc10_0)),
            ("pm0_1"                        ,self.cor_pm0_1), 
            ("pm0_3"                        ,self.cor_pm0_3),
            ("pm0_5"                        ,self.cor_pm0_5),
            ("pm1_0"                        ,self.cor_pm1_0),
            ("pm2_5"                        ,self.cor_pm2_5),
            ("pm5_0"                        ,self.cor_pm5_0), 
            ("pm10_0"                       ,self.cor_pm10_0),
            ("temperature"                  ,self.temperature),
            ("pressure"                     ,self.pressure), 
            ("humidity"                     ,self.humidity),
            ("dewPoint"                     ,self.dewPoint),
            ("temperatureValidity"          ,self.temperatureValidity), 
            ("humidityValidity"             ,self.humidityValidity),
            ("momentaryValidity"            ,self.momentaryValidity),
            ("humidityLikelyhoodValidity"   ,self.humidityLikelyhoodValidity),
            ("dewPointValidity"             ,self.dewPointValidity),
            ("correctionRequirment"         ,self.correctionRequirment)
            ])
        
        print(sensorDictionary)


      
        print()        
        print("===============MINTS===============")
        # print(sensorDictionary)

        # ADJUST THE SENSOR ID HERE
        # mP.writeCSV3( mP.getWritePathDateCSV(liveFolder,self.nodeID,\
        #     datetime.strptime(self.dateTimeStrCSV,'%Y-%m-%d %H:%M:%S.%f'),\
        #         "calibrated"),sensorDictionary)
        print("CSV Written")
        # mL.writeMQTTLatestRepublish(sensorDictionary,"mintsCalibrated",self.nodeID)=





    




