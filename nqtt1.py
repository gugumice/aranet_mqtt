#!/usr/bin/env python3

import paho.mqtt.client as mqtt
import logging
import random
from datetime import datetime,timedelta
from time import sleep
import csv
import json

csvfile_name='mqtt.csv'
sensors={}
mqtt_broker_adr = "10.100.107.199"
mqtt_broker_port = 8883
client_id="{}-{:04d}".format("MQTT",random.randint(0,9999))
run_time = timedelta(seconds=15*60)
logging.basicConfig(level=logging.DEBUG)

def main():
    start_time=datetime.now()
    client = mqtt.Client(client_id=client_id,clean_session=True)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(mqtt_broker_adr, mqtt_broker_port, 60)
    #client.loop_forever()
    
    while (start_time+run_time > datetime.now()):
        client.loop()

    print('\nEnd')
    client.disconnect()
    write_to_file(sensors,csvfile_name)

def write_to_file(sd: dict, csvfile_name: str):
    fields = ['SensorId','SensorName','Group','rssi','Batt%']
    with open(csvfile_name, 'w') as csvfile:
        writer=csv.writer(csvfile)
        writer.writerow(fields)
        for k,v in sd.items():
            row=[k,v['name'],v['group']]
            if 'measurements' in v:
                row=row+[v['measurements']['rssi'],v['measurements']['battery']]
            print(row)
            writer.writerow(row)

def on_connect(client, userdata, flags, rc):
    logging.info('Connected with result code {}'.format('rc'))
    connstr = [('Aranet/+/sensors/+/name',0),('Aranet/+/sensors/+/json/measurements',0),('Aranet/+/sensors/+/group',0)]
    client.subscribe(connstr)

def on_message(client, userdata, msg):
    rec_message="{}\t{}\t{}".format(msg.topic,msg.payload.decode('utf-8'),str(msg.qos))
    logging.info(rec_message)
    make_list(msg.topic,msg.payload)

def make_list(topic, payload):
    '''
    Parses MQTT messages, writes to sensors dict
    '''
    global sensors
    payload=payload.decode('utf-8')
    #get sensor MAC address as primary key
    sens_id=topic.split('/')[3]
    try:
        if not sens_id in sensors:
            sensors[sens_id]={}
        if topic.endswith('/name'):
            sensors[sens_id]['name']=payload
        elif topic.endswith('/group'):
            sensors[sens_id]['group'] = payload
        elif topic.endswith('/measurements'):
            sensors[sens_id]['measurements'] = json.loads(payload)
    except KeyError as ke:
        logging.ERROR(ke)
    
if __name__ == "__main__":
    main()