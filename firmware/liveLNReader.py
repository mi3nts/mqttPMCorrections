# MQTT Client demo
# Continuously monitor two different MQTT topics for data,
# check if the received data matches two predefined 'commands'
import base64
from pickle import TRUE
import paho.mqtt.client as mqtt
import datetime
import yaml
import collections
import json
from collections import OrderedDict
import struct



from mintsXU4 import mintsSensorReader as mSR
from mintsXU4 import mintsDefinitions as mD
from mintsXU4 import mintsLatest as mL
from mintsXU4 import mintsLoRaReader as mLR
from mintsXU4 import mintsProcessing as mP
from mintsXU4 import mintsLiveNodes as mLN



mqttPort              = mD.mqttPortLoRa
mqttBroker            = mD.mqttBrokerLoRa
mqttCredentialsFile   = mD.credentials
tlsCert               = mD.tlsCert
nodeInfo              = mD.nodeInfo
sensorInfo            = mD.sensorInfo
portInfo              = mD.portInfo
credentials           = mD.credentials

connected             = False  # Stores the connection status
broker                = mqttBroker  
port                  = mqttPort  # Secure port
mqttUN                = credentials['LoRaMqtt']['username'] 
mqttPW                = credentials['LoRaMqtt']['password'] 

nodeIDs               = nodeInfo['nodeID']
print(nodeIDs)
sensorIDs             = sensorInfo['sensorID']
portIDs               = portInfo['portID']

# currentState = 0
nodeObjects  = []

    
def getNodeIndex(nodeIDRead):
    indexOut = 0
    for nodeID in nodeIDs:
        if (nodeIDRead == nodeID):
            return indexOut; 
        indexOut = indexOut +1
    return -1;

decoder = json.JSONDecoder(object_pairs_hook=collections.OrderedDict)

def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    topic = "utd/lora/app/5/device/+/event/up"
    client.subscribe(topic)
    print("Subscrbing to Topic: "+ topic)

    print("Initializing Node Objects")
    for index, nodeInfoRow in nodeInfo.iterrows():
        print(nodeInfoRow)
        nodeObjects.append(mLN.node(nodeInfoRow))
        print("------------------------------------------------------")
        print("------------------------------------------------------")        


def on_message(client, userdata, msg):
 
    try:
    # if (True):
        print("==================================================================")
        print(" - - - MINTS DATA RECEIVED - - - ")
        # print(msg.payload)
        
        dateTime,gatewayID,nodeID,sensorID,framePort,base16Data = \
            mLR.loRaSummaryWrite(msg,portInfo)
        print("Node ID         : " + nodeID)
        print("Sensor ID       : " + sensorID)

        if nodeID is not None:
            currentTimeInSec  = dateTime.timestamp()
            liveState         = mP.getStateV2(currentTimeInSec)
            nodeIndex         = getNodeIndex(nodeID)

            sensorDictionary  = mLR.sensorSendLoRa(dateTime,nodeID,sensorID,framePort,base16Data)
           
            if nodeIndex>=0 : 
                print() 
                print("==================================================================")
                print(" - - - ---------------- MINTS DATA RECEIVED ----------------- - - ")             
                print(" ------------------- Data for Live Node found ------------------- ")
                # print("Node ID         : " + nodeID)
                # print("Sensor ID       : " + sensorID)
                # print("Node Index      : " + str(nodeIndex))
                # print("LN Date Time    : " + str(dateTime))
                # print("Sensor Data     : " +  str(sensorDictionary))
                # print()
                # print(" - - - MINTS NODE RECOGNIZED - - - ")
                # print("Data      :" + str(sensorDictionary))
                nodeObjects[nodeIndex].update(sensorID,sensorDictionary)
                
            else:
                print("Node ID not registered: {}".format(nodeID)) 

        else:
            print("Invalid data received")
            
    except Exception as e:
        print("[ERROR] Could not publish data, error: {}".format(e))


# Create an MQTT client and attach our routines to it.
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.username_pw_set(mqttUN,mqttPW)
client.connect(broker, port, 60)
client.loop_forever()

