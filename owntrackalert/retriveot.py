import paho.mqtt.client as mqtt
import json
import os
import logging
import signal
import requests
from datetime import datetime
from dotenv import find_dotenv, load_dotenv
import sqlite3


load_dotenv(find_dotenv())
MQTT_HOST = os.getenv("DST_MQTT_HOST")
MQTT_USER = os.getenv("DST_MQTT_USER")
MQTT_PASS = os.getenv("DST_MQTT_PASS")

VERSION = "v1.0"
CON = False
OT_TID="dragino"

def on_connect_ot(client, userdata, flags, rc):
    logging.info("connected to ot %s - %s", MQTT_HOST, str(rc))
    client.subscribe("owntracks/+/+/+")


def on_publish_ot(client, userdata, rc):
    logging.info("published data to ot")

def on_log(client, userdata, level, buf):
    logging_level = mqtt.LOGGING_LEVEL[level]
    logging.log(logging_level, buf)

def CreateUpdateUser(cur, tid):
    cur.execute("SELECT id FROM users where name =?", (tid,))
    
    rows = cur.fetchall()
    if rows:
        return rows[0][0]
    else:
        cur.execute("INSERT INTO users (name) VALUES (?)", (tid,))
        return cur.lastrowid


# The callback for when a PUBLISH message is received from the server.
def on_message_ot(client, userdata, msg):
    data = json.loads(msg.payload)
    if data['_type'] == 'location':
        logging.info("message from ttn received for %s", data["tid"])
        cur = CON.cursor()
        user_id = CreateUpdateUser(cur, data["tid"])
        sql_record = {
        "accuracy": data["acc"],
        "altitude":data["alt"],
        "battery":data["batt"],
        "latitude":data["lat"],
        "longitude":data["lon"],
        "trackerid":data["tid"],
        "timestamp":data["tst"],
        "verticalaccuracy":data["vac"],
        "velocity":data["vel"],
        "connection":data["conn"],
        "userid":user_id,
        
        }
 

        columns = ', '.join(sql_record.keys())
        placeholders = ', '.join('?' * len(sql_record))
        sql = 'INSERT INTO points ({}) VALUES ({})'.format(columns, placeholders)
        values = [int(x) if isinstance(x, bool) else x for x in sql_record.values()]
        cur.execute(sql, values)
        CON.commit()

        logging.info("data processing done")
    elif data['_type'] == 'lwt':
        logging.info("Lost connection")
    elif data['_type'] == 'waypoint':
        logging.info("Lost connection")
        sql_record = {
            "longitude": data["lon"],
            "latitude": data["lat"],
            "name": data["desc"],
            "radius": data["radius"],
            "comment": "",
        }
        cur = CON.cursor()

        columns = ', '.join(sql_record.keys())
        placeholders = ', '.join('?' * len(sql_record))
        sql = 'INSERT INTO waypoints ({}) VALUES ({})'.format(columns, placeholders)
        values = [int(x) if isinstance(x, bool) else x for x in sql_record.values()]
        cur.execute(sql, values)
        CON.commit()

def shutdown():
    logging.info("disconnecting from mqtt")
    client_ot.disconnect()
    client_ot.loop_stop()
    CON.close()

def handleSIGTERM(signalNumber, frame):
    logging.info("got SIGTERM")
    shutdown()
    return

if __name__ == '__main__':

    signal.signal(signal.SIGTERM, handleSIGTERM)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S %Z'
    )

    logging.info("Starting ioteer lgt92. "+VERSION)

    # Prepare MQTT for The Things Network
    CON = sqlite3.connect('/tmp/owntrackstore')
    client_ot = mqtt.Client()
    client_ot.enable_logger()
    client_ot.on_connect = on_connect_ot
    client_ot.on_message = on_message_ot
    client_ot.on_publish = on_publish_ot
    client_ot.on_log = on_log
    client_ot.username_pw_set(MQTT_USER,MQTT_PASS)
    #client_ot.tls_set()
    #client_ot.will_set(OT_TOPIC, payload=ot_lwt, qos=1, retain=True)
    client_ot.connect(MQTT_HOST, 8883, 60)
    
    try:
        # Connect to MQTT and react to messages
        logging.info("Start waiting messages")
        client_ot.loop_forever()
    except KeyboardInterrupt:
        shutdown()
        logging.info("Shutting down")

